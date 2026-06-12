package com.example.topology;

import com.example.model.BookingAggregate;
import com.example.serde.BookingAggregateSerde;
import com.example.serde.JsonNodeSerde;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import org.jboss.logging.Logger;

@ApplicationScoped
public class BookingPassengerTopology {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG =
            Logger.getLogger(BookingPassengerTopology.class);

    @Produces
    public Topology buildTopology() {

        // Booking Topic
        StreamsBuilder builder = new StreamsBuilder();


        KTable<Long, JsonNode> bookingTable =
                builder.<String, String>stream(
                                "booking-topic",
                                Consumed.with(
                                        Serdes.String(),
                                        Serdes.String()
                                )
                        )
                        .peek((key, value) ->
                                LOG.infof(
                                        "BOOKING MESSAGE RECEIVED -> %s",
                                        value
                                )
                        )
                        .selectKey(
                                (key, value) ->
                                        Long.valueOf(
                                                JsonPath.read(
                                                        value,
                                                        "$.message.data.BookingID"
                                                ).toString()
                                        )
                        )
                        .mapValues(this::extractDataNode)
                        .toTable(
                                Materialized.with(
                                        Serdes.Long(),
                                        new JsonNodeSerde()
                                )
                        );

        // Booking Passenger Topic

        KTable<Long, JsonNode> passengerTable =
                builder.<String, String>stream(
                                "booking-passenger-topic",
                                Consumed.with(
                                        Serdes.String(),
                                        Serdes.String()
                                )
                        )
                        .peek((key, value) ->
                                LOG.infof(
                                        "PASSENGER MESSAGE RECEIVED -> %s",
                                        value
                                )
                        )
                        .selectKey(
                                (key, value) ->
                                        Long.valueOf(
                                                JsonPath.read(
                                                        value,
                                                        "$.message.data.BookingID"
                                                ).toString()
                                        )
                        )
                        .mapValues(this::extractDataNode)
                        .toTable(
                                Materialized.with(
                                        Serdes.Long(),
                                        new JsonNodeSerde()
                                )
                        );

        // Join

        KTable<Long, BookingAggregate> aggregateTable =
                bookingTable.leftJoin(
                        passengerTable,
                        (booking, passenger) -> {

                            BookingAggregate aggregate =
                                    new BookingAggregate();

                            if (booking != null) {

                                aggregate.setBookingId(
                                        booking.path("BookingID")
                                                .asLong()
                                );

                                aggregate.setBooking(booking);
                            }

                            aggregate.setPassenger(passenger);

                            LOG.infof(
                                    "AGGREGATE CREATED -> BookingId=%s PassengerPresent=%s",
                                    aggregate.getBookingId(),
                                    passenger != null
                            );

                            return aggregate;
                        }
                );

        // Convert to stream

        KStream<Long, BookingAggregate> aggregateStream =
                aggregateTable.toStream();

        // Publish

        aggregateStream
                .peek((key, value) ->
                        LOG.infof(
                                "PUBLISHING TO booking-aggregate-topic -> BookingId=%s Aggregate=%s",
                                key,
                                value
                        )
                )
                .to(
                        "booking-aggregate-topic-new",

                        Produced.with(
                                Serdes.Long(),
                                new BookingAggregateSerde()
                        )
                );

        return builder.build();

    }

    private JsonNode extractDataNode(String json) {

        try {

            JsonNode root = mapper.readTree(json);

            return root.path("message")
                    .path("data");

        } catch (Exception e) {

            throw new RuntimeException(
                    "Unable to parse message",
                    e
            );
        }
    }
}