package com.example.broker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

@ApplicationScoped
public class ScheduledMessageManager {

    @Inject
    ConnectionFactory connectionFactory;

    private static final String MANAGEMENT_ADDRESS = "activemq.management";
    private final ObjectMapper mapper = new ObjectMapper();

    /** Finds and deletes a not-yet-delivered scheduled message matching flightNumber. */
    public boolean cancelScheduledMessage(String queueName, String flightNumber) throws Exception {
        try (JMSContext context = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
            String json = invokeManagementOp(context, queueName, "listScheduledMessagesAsJSON", "[]");
            JsonNode messages = mapper.readTree(json);

            for (JsonNode msg : messages) {
                JsonNode props = msg.get("StringProperties");
                if (props != null && flightNumber.equals(textOrNull(props.get("flightNumber")))) {
                    long messageId = msg.get("JMSMessageID") != null
                            ? msg.get("messageID").asLong()   // Artemis returns "messageID" (internal, numeric)
                            : msg.get("messageID").asLong();
                    invokeManagementOp(context, queueName, "deleteMessage", "[" + messageId + "]");
                    return true;
                }
            }
            return false; // nothing found to cancel — might already have fired, or never existed
        }
    }

    private String invokeManagementOp(JMSContext context, String queueName, String operation, String jsonArgs) {
        Queue managementQueue = context.createQueue(MANAGEMENT_ADDRESS);
        Queue replyQueue = context.createTemporaryQueue();

        Message message = context.createMessage();
        try {
            message.setStringProperty("_AMQ_ResourceName", "queue." + queueName);
            message.setStringProperty("_AMQ_OperationName", operation);
            message.setJMSReplyTo(replyQueue);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        message.setBody(String.class, jsonArgs);

        context.createProducer().send(managementQueue, message);

        JMSConsumer consumer = context.createConsumer(replyQueue);
        Message reply = consumer.receive(5000);
        return reply != null ? reply.getBody(String.class) : "[]";
    }

    private String textOrNull(JsonNode node) {
        return node != null ? node.asText() : null;
    }
}