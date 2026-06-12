package com.example.utility;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class MyMessagingApplication extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        // ---------------------------------------------------------
        // REST Configuration
        // ---------------------------------------------------------
        restConfiguration()
                .component("platform-http")
                .contextPath("/api")
                .port(8080);

        rest("/booking-topic")
                .post()
                .produces("text/plain")
                .to("direct:kafkaTopic");

        rest("/booking-passenger-topic")
                .post()
                .produces("text/plain")
                .to("direct:kafkaTopic-passenger");

        // ---------------------------------------------------------
        // Kafka Producer Route
        // ---------------------------------------------------------
        from("direct:kafkaTopic")
                .routeId("kafka-producer")
                .to("kafka:booking-topic"
                        + "?brokers={{kafka.bootstrap.servers}}"
                        + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                        + "&sslTruststorePassword=password")
                .log("Sent to booking Kafka");

        from("direct:kafkaTopic-passenger")
                .routeId("kafka-producer-passenger")
                .to("kafka:booking-passenger-topic"
                        + "?brokers={{kafka.bootstrap.servers}}"
                        + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                        + "&sslTruststorePassword=password")
                .log("Sent to payment Kafka");

    }
}