import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.aws.s3.S3Component;
import org.apache.camel.component.aws.s3.S3Constants;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.processor.idempotent.MemoryIdempotentRepository;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.util.FileUtil;

/**
 *
 */
public class ComedyGenreHandler extends RouteBuilder {

	public void configure() {

		PropertiesComponent pc = getContext().getComponent("properties", PropertiesComponent.class);

		//no error handler for this route
		errorHandler(noErrorHandler());

		S3Component s3Component = getContext().getComponent("aws-s3", S3Component.class);
		s3Component.getConfiguration().setAmazonS3Client(amazonS3Client(pc));

		/**
		 * Process data request body the end point
		 * TODO:
		 * this needs to be extracted from properties, currently knative endpoint is not mounting the volume
		 * this will be enabled once camel-k enables volume mounts for knative components
		 *
		 */
		// @formatter:off
	   from("knative:channel/messages-us")
			   .log(LoggingLevel.DEBUG,
					   "Received content ${body}")
				 .setProperty("userName", xpath("/person/@user", String.class))
				 .convertBodyTo(byte[].class)
				 .log(LoggingLevel.DEBUG,"Writing file {{messagesDir}}/${property.userName}.xml")
				 .to("file://{{messagesDir}}/us?fileName=${property.userName}.xml")
				 .end();

	    from("file:{{messagesDir}}?noop=true&recursive=true")
					.log(LoggingLevel.DEBUG,"Processing file ${in.header.CamelFileName}")
				 .setHeader(S3Constants.CONTENT_LENGTH, simple("${in.header.CamelFileLength}"))
                .setHeader(S3Constants.KEY, simple("${in.header.CamelFileNameOnly}"))
                .convertBodyTo(byte[].class)
                .process(exchange -> {
                    //Build a valid destination bucket name
                    String camelFileRelativePath = exchange.getIn().getHeader("CamelFileRelativePath", String.class);
                    String onlyPath = "us-out";
                    if (camelFileRelativePath != null) {
                        log.debug("Camel File Relative Path " + camelFileRelativePath);
                        onlyPath = FileUtil.onlyPath(camelFileRelativePath);
                        log.debug("Camel File  onlyPath " + onlyPath);
                    }
                    exchange.setProperty("toBucketName", "messages-"+ onlyPath.toLowerCase());
                })
								.log("Uploading file ${header.CamelFileName} to bucket: ${property.toBucketName}")
                .toD("aws-s3://${property.toBucketName}?deleteAfterWrite=false")
        .end();
		// @formatter:on
	}

	AmazonS3 amazonS3Client(PropertiesComponent pc) {

		final String s3EndpointUrl = property(pc, "s3EndpointUrl", "http://minio-server");
		log.info("S3 URL -> " + s3EndpointUrl);

		final String minioAccessKey = property(pc, "minioAccessKey", "demoaccesskey");
		final String minioSecretKey = property(pc, "minioSecretKey", "demosecretkey");

		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setSignerOverride("AWSS3V4SignerType");

		AWSCredentials credentials = new BasicAWSCredentials(minioAccessKey, minioSecretKey);
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

		AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
				                                      .withCredentials(credentialsProvider).withClientConfiguration(clientConfiguration)
				                                      .withPathStyleAccessEnabled(true)
				                                      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3EndpointUrl,
						                                      Regions.US_EAST_1.name()));

		return clientBuilder.build();
	}

	/**
	 * @param pc
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private String property(PropertiesComponent pc, String key, String defaultValue) {
		try {
			getContext().resolvePropertyPlaceholders(pc.getPropertyPrefix() + key + pc.getPropertySuffix());
			if (System.getenv().containsKey(key)) {
				return System.getenv().getOrDefault(key, defaultValue);
			}
		} catch (Exception e) {

		} finally {
			return defaultValue;
		}
	}

	IdempotentRepository idmRepo() {
		return new MemoryIdempotentRepository();
	}
}
