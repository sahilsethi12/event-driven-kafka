package com.example.broker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;

@ApplicationScoped
public class ScheduledMessageSender {

    @Inject
    ConnectionFactory connectionFactory;

    /**
     * Sends a message with a per-message delivery delay.
     *
     * @param body        the message body
     * @param queueName   target queue name
     * @param delayMillis how long to delay delivery, in milliseconds
     */

    public void sendScheduled(String body, String queueName, long delayMillis) {
        try (JMSContext context = connectionFactory.createContext()) {
            Queue queue = context.createQueue(queueName);
            JMSProducer producer = context.createProducer()
                    .setDeliveryDelay(delayMillis);
            producer.send(queue, body);
        }
    }
}