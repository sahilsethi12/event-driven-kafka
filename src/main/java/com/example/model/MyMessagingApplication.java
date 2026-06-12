package com.example.model;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class MyMessagingApplication extends RouteBuilder {

    @Inject
    CorrelationStore store;

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

        rest("/payment-topic")
                .post()
                .produces("text/plain")
                .to("direct:kafkaTopic-payment");

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

        from("direct:kafkaTopic-payment")
                .routeId("kafka-producer-payment")
                .to("kafka:payment-topic"
                        + "?brokers={{kafka.bootstrap.servers}}"
                        + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                        + "&sslTruststorePassword=password")
                .log("Sent to payment Kafka");

        // ---------------------------------------------------------
        // Booking Topic Consumer
        // ---------------------------------------------------------

                /*from("kafka:booking-topic"
                        + "?brokers={{kafka.bootstrap.servers}}"
                        + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                        + "&sslTruststorePassword=password")
                .routeId("booking-route")

                .unmarshal().json(Booking.class)

                .process(exchange -> {

                    Booking booking =
                            exchange.getMessage().getBody(Booking.class);

                    store.saveBooking(booking);

                    Payment payment =
                            store.getPayment(booking.getBookingId());

                    if (payment != null) {

                        BookingPayment agg =
                                new BookingPayment();

                        agg.setBookingId(
                                booking.getBookingId());

                        agg.setCustomerName(
                                booking.getCustomerName());

                        agg.setFlightNumber(
                                booking.getFlightNumber());

                        agg.setAmount(
                                payment.getAmount());

                        agg.setPaymentStatus(
                                payment.getStatus());

                        exchange.getMessage().setBody(agg);

                    } else {
                        exchange.setProperty("skipPublish", true);
                    }
                })

                .choice()
                .when(exchangeProperty("skipPublish").isNull())
                .marshal().json()
                .to("kafka:booking-test-topic"
                        + "?brokers={{kafka.bootstrap.servers}}"
                        + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                        + "&sslTruststorePassword=password")
                .log("Booking aggregate sent to Kafka")
                .end();
*/
        // ---------------------------------------------------------
        // Payment Topic Consumer
        // ---------------------------------------------------------
       /* from("kafka:payment-topic"
                + "?brokers={{kafka.bootstrap.servers}}"
                + "&securityProtocol=SSL"
                + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                + "&sslTruststorePassword=password")
                .routeId("payment-route")

                .unmarshal().json(Payment.class)

                .process(exchange -> {

                    Payment payment =
                            exchange.getMessage().getBody(Payment.class);

                    store.savePayment(payment);

                    Booking booking =
                            store.getBooking(payment.getBookingId());

                    if (booking != null) {

                        BookingPayment agg =
                                new BookingPayment();

                        agg.setBookingId(
                                booking.getBookingId());

                        agg.setCustomerName(
                                booking.getCustomerName());

                        agg.setFlightNumber(
                                booking.getFlightNumber());

                        agg.setAmount(
                                payment.getAmount());

                        agg.setPaymentStatus(
                                payment.getStatus());

                        exchange.getMessage().setBody(agg);

                    } else {
                        exchange.setProperty("skipPublish", true);
                    }
                })

                .choice()
                .when(exchangeProperty("skipPublish").isNull())
                .marshal().json()
                .to("kafka:booking-aggregate-topic"
                        + "?brokers={{kafka.bootstrap.servers}}"
                        + "&securityProtocol=SSL"
                        + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                        + "&sslTruststorePassword=password")
                .log("Payment aggregate published")
                .end();
*/

/*        from("kafka:booking-aggregate-topic"
                + "?brokers={{kafka.bootstrap.servers}}"
                + "&securityProtocol=SSL"
                + "&sslTruststoreLocation=/Users/sasethi/event-driven-kafka/kafka.truststore.jks"
                + "&sslTruststorePassword=password")
                .routeId("booking-aggregate")
                .log("Hello In aggregrate topic ${body}");*/

    }
}