package com.example.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {

        try {
            if (data == null || data.length == 0) {
                return null;
            }

            return MAPPER.readValue(data, clazz);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to deserialize to " + clazz.getName(),
                    e
            );
        }
    }
}