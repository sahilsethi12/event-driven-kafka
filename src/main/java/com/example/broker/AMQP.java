package com.example.broker;


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import org.apache.camel.component.amqp.AMQPComponent;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class AMQP {

    @ConfigProperty(name = "amqp.url")
    String brokerUrl;

    @ConfigProperty(name = "amqp.username")
    String username;

    @ConfigProperty(name = "amqp.password")
    String password;

//    @Produces
//    @Named("amqp")
//    public AMQPComponent amqpComponent() {
//        JmsConnectionFactory connectionFactory = new JmsConnectionFactory(username, password, brokerUrl);
//        AMQPComponent component = new AMQPComponent();
//        component.setConnectionFactory(connectionFactory);
//        return component;
//    }
}
