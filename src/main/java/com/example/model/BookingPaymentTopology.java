package com.example.model;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import org.jboss.logging.Logger;

@ApplicationScoped
public class BookingPaymentTopology {

    private static final Logger LOG =
            Logger.getLogger(BookingPaymentTopology.class);

    @Produces
    public Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<Booking> bookingSerde = new ObjectMapperSerde<>(Booking.class);

        ObjectMapperSerde<Payment> paymentSerde = new ObjectMapperSerde<>(Payment.class);

        ObjectMapperSerde<BookingPayment> resultSerde = new ObjectMapperSerde<>(BookingPayment.class);

        KTable<String, Booking> bookingTable =
                builder.stream(
                                "booking-topic",
                                Consumed.with(
                                        Serdes.String(),
                                        bookingSerde))
                        .peek((key, value) ->
                                LOG.infof(
                                        "BOOKING RECEIVED -> KafkaKey=%s bookingId=%s customer=%s",
                                        key,
                                        value.getBookingId(),
                                        value.getCustomerName()))
                        .selectKey((k, v) -> {
                            LOG.infof(
                                    "BOOKING REKEY -> oldKey=%s newKey=%s",
                                    k,
                                    v.getBookingId());

                            return v.getBookingId();
                        })
                        .toTable(
                                Materialized.with(
                                        Serdes.String(),
                                        bookingSerde
                                )
                        );

        KTable<String, Payment> paymentTable =
                builder.stream(
                                "payment-topic",
                                Consumed.with(
                                        Serdes.String(),
                                        paymentSerde))
                        .peek((key, value) ->
                                LOG.infof(
                                        "PAYMENT RECEIVED -> KafkaKey=%s bookingId=%s amount=%s",
                                        key,
                                        value.getBookingId(),
                                        value.getAmount()))
                        .selectKey((k, v) -> {
                            LOG.infof(
                                    "PAYMENT REKEY -> oldKey=%s newKey=%s",
                                    k,
                                    v.getBookingId());

                            return v.getBookingId();
                        })
                        .toTable(
                                Materialized.with(
                                        Serdes.String(),
                                        paymentSerde
                                )
                        );

        KTable<String, BookingPayment> joinedTable =
                bookingTable.join(
                        paymentTable,
                        (booking, payment) -> {

                            LOG.infof(
                                    "JOIN EXECUTED bookingId=%s",
                                    booking.getBookingId()
                            );

                            return new BookingPayment(
                                    booking.getBookingId(),
                                    booking.getCustomerName(),
                                    payment.getAmount()
                            );
                        }
                );

        joinedTable
                .toStream()
                .peek((key, value) ->
                        LOG.infof(
                                "PUBLISHING -> topic=booking-aggregate-topic bookingId=%s customer=%s amount=%s",
                                value.getBookingId(),
                                value.getCustomerName(),
                                value.getAmount()))
                .to(
                        "booking-aggregate-topic",
                        Produced.with(
                                Serdes.String(),
                                resultSerde
                        )
                );

        return builder.build();
    }
}