package com.example.utility;

import com.example.broker.FlightRescheduleService;
import com.example.broker.ScheduledMessageSender;
import jakarta.enterprise.context.ApplicationScoped;

import jakarta.inject.Inject;
import org.apache.camel.builder.RouteBuilder;

import java.time.OffsetDateTime;

@ApplicationScoped
public class MyMessagingApplication extends RouteBuilder {

    @Inject
    ScheduledMessageSender scheduledMessageSender;

    @Override
    public void configure() throws Exception {



        // ---------------------------------------------------------
        // REST Configuration
        // ---------------------------------------------------------
        restConfiguration()
                .component("platform-http")
                .contextPath("/api")
                .port(8080);


        rest("/test")
                .post()
                .to("direct:push-to-broker");

        rest("/testnew")
                .post()
                .to("direct:reschedule-flight");


        from("direct:push-to-broker")
                .log("Sending at: ${date:now}")
                .setHeader("flightTime", jsonpath("$.flightTime"))
                .setHeader("flightNumber").jsonpath("$.flightNumber")
              //  .bean(ScheduledMessageSender.class,
                //        "sendScheduled(${body}, 'test.queue', ${header.delayMillis})")
                //   .to("amqp:queue:test.queue?exchangePattern=InOnly")
                .process(exchange -> {
                    String flightTime = exchange.getMessage().getHeader("flightTime", String.class);
                    OffsetDateTime flightDateTime = OffsetDateTime.parse(flightTime);
                    long deliveryTimeMillis = flightDateTime.minusMinutes(10).toInstant().toEpochMilli();

                    long delayMillis = deliveryTimeMillis - System.currentTimeMillis();
                    // Don't use a negative delay
                    if (delayMillis < 0) {
                        delayMillis = 0;
                    }
                    exchange.getMessage().setHeader("delayMillis", delayMillis);
                })
                .log("Flight ${header.flightNumber}")
                .log("Flight time: ${header.flightTime}")
                .log("Delay: ${header.delayMillis} ms")
                .toD("amqp:queue:test.queue?exchangePattern=InOnly&deliveryDelay=${header.delayMillis}")
                .log("Sent")
                .setBody(simple("Hello from Camel - scheduled message for ${header.flightNumber}"))
        ;

        from("direct:update-flight-schedule")
                .setHeader("flightTime", jsonpath("$.flightTime"))
                .setHeader("flightNumber", jsonpath("$.flightNumber"))
                .process(exchange -> {
                    String flightNumber = exchange.getMessage().getHeader("flightNumber", String.class);
                    boolean cancelled = scheduledMessageManager.cancelScheduledMessage("test.queue", flightNumber);
                    exchange.getMessage().setHeader("wasCancelled", cancelled);
                })
                .log("Cancelled previous schedule for ${header.flightNumber}: ${header.wasCancelled}")
                // reuse your existing delay-calculation + send logic
                .to("direct:push-to-broker");

        from("amqp:queue:test.queue").autoStartup(true)
                .log("Received at: ${date:now}")
                .log("Headers: ${headers}");
    }

}