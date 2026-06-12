package com.example.serde;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;

public class JsonNodeSerde extends ObjectMapperSerde<JsonNode> {

    public JsonNodeSerde() {
        super(JsonNode.class);
    }
}