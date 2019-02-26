package com.example.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.knative.KnativeComponent;
import org.apache.camel.component.knative.KnativeEnvironment;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.camel.main.Main;
import org.apache.camel.test.AvailablePortFinder;

import java.util.Collections;

/**
 * A Camel Application
 */
public class QuickStartApp {


//    public static void main(String... args) throws Exception {
//        Main main = new Main();
//        PropertiesComponent pc = new PropertiesComponent();
//        pc.setLocation("application-local.properties");
//        main.bind("properties",pc);
//        main.addRouteBuilder(new QuickStartRoute());
//        main.run(args);
//    }

    public static void main(String[] args) throws Exception {

        SimpleRegistry registry = new SimpleRegistry();
        DefaultCamelContext context = new DefaultCamelContext(registry);


        KnativeEnvironment env = new KnativeEnvironment(Collections.emptyList());

        KnativeComponent component = context.getComponent("knative", KnativeComponent.class);
        component.setEnvironment(env);

        int port =  AvailablePortFinder.getNextAvailable();

        try {
            context.disableJMX();
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {

                    fromF("netty4-http:http://localhost:%d/a/path",port)
                        .convertBodyTo(String.class)
                        .to("log:info?showAll=true&multiline=true");

                    from("timer:period=5s")
                        .setBody(constant("Hare Krishna!"))
                        .toF("netty4-http:http://localhost:%d/a/path",port);
//
//                    from("knative:channel/dummy")
//                        .to("log:info");
                }
            });

            context.start();

            Thread.sleep(Integer.MAX_VALUE);
        } finally {
            context.stop();
        }

    }

}

