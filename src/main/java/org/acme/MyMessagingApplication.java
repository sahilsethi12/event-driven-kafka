package org.acme;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;


import java.util.stream.Stream;

@ApplicationScoped
public class MyMessagingApplication extends RouteBuilder  {


    @Override
    public void configure() throws Exception {


        from("timer:foo?period=10000")
                .setBody(constant("Hello from Camel Quarkus!"))
                .to("log:example-logger");
    }
}
