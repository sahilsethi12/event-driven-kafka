package com.example.broker;


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;

import java.time.OffsetDateTime;

@ApplicationScoped
public class FlightRescheduleService {

    @Inject
    ConnectionFactory connectionFactory;

    public void reschedule(
            String flightNumber,
            String newFlightTime) {

        long newDeliveryTime =
                OffsetDateTime.parse(newFlightTime).minusMinutes(10).toInstant().toEpochMilli();

        long newDelay = Math.max(0, newDeliveryTime - System.currentTimeMillis());

        System.out.println("Rescheduling flight " + flightNumber);

        System.out.println("New flight time: " + newFlightTime);

        System.out.println("New delivery time: " + newDeliveryTime);

        System.out.println("New delay: " + newDelay);
    }
}
