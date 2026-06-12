package com.example.serde;

import com.example.model.BookingAggregate;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;

public class BookingAggregateSerde
        extends ObjectMapperSerde<BookingAggregate> {

    public BookingAggregateSerde() {
        super(BookingAggregate.class);
    }
}