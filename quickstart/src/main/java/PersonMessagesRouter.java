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
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.processor.idempotent.MemoryIdempotentRepository;
import org.apache.camel.spi.IdempotentRepository;

/**
 * A Camel Java DSL Router
 */
public class PersonMessagesRouter extends RouteBuilder {

	public void configure() {

		PropertiesComponent pc = (PropertiesComponent) getContext().getComponent("properties");

		S3Component s3Component = getContext().getComponent("aws-s3", S3Component.class);
		s3Component.getConfiguration().setAmazonS3Client(amazonS3Client(pc));


		//no error handler for this route
		errorHandler(noErrorHandler());

		/**
		 * Process data request body the end point
		 * TODO - this needs to be extracted from properties, currently knative endpoint is not mounting the volume
		 */
		// @formatter:off
		from("aws-s3://data?deleteAfterRead=false")
				.streamCaching()
				.filter(header("CamelAwsS3Key").endsWith(".xml"))
				.idempotentConsumer(header("CamelAwsS3ETag"), idmRepo())
				.log(LoggingLevel.DEBUG,"Processing File : ${header.CamelAwsS3Key}")
				.choice()
				.when(xpath("/person/country = 'US'"))
				    .log("Sending Body ${body}")
					  .to("knative:channel/messages-us")
				.otherwise()
				    .log("Sending Body ${body}")
						.to("knative:channel/messages-others")
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
						                                      Regions.AP_SOUTH_1.name()));

		return clientBuilder.build();
	}

	/**
	 * @param propertiesComponent
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private static String property(PropertiesComponent propertiesComponent, String key, String defaultValue) {
		try {
			if (System.getenv().containsKey(key)) {
				return System.getenv().getOrDefault(key, defaultValue);
			} else {
				return propertiesComponent.parseUri(propertiesComponent.getPrefixToken() + key + propertiesComponent.getSuffixToken());
			}
		} catch (IllegalArgumentException e) {
			return defaultValue;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	IdempotentRepository idmRepo() {
		return new MemoryIdempotentRepository();
	}

}
