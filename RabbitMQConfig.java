package com.balanceiq.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // --- RAG Sync ---
    public static final String RAG_SYNC_QUEUE    = "rag.sync.queue";
    public static final String RAG_SYNC_EXCHANGE = "rag.sync.exchange";
    public static final String RAG_SYNC_KEY      = "rag.sync";

    // --- Notifications ---
    public static final String NOTIFICATION_QUEUE    = "notification.queue";
    public static final String NOTIFICATION_EXCHANGE = "notification.exchange";
    public static final String NOTIFICATION_KEY      = "notification.create";

    @Bean
    public Queue ragSyncQueue() {
        // durable=true so messages survive RabbitMQ restarts
        return QueueBuilder.durable(RAG_SYNC_QUEUE).build();
    }

    @Bean
    public TopicExchange ragSyncExchange() {
        return new TopicExchange(RAG_SYNC_EXCHANGE);
    }

    @Bean
    public Binding ragSyncBinding(Queue ragSyncQueue, TopicExchange ragSyncExchange) {
        return BindingBuilder.bind(ragSyncQueue).to(ragSyncExchange).with(RAG_SYNC_KEY);
    }

    @Bean
    public Queue notificationQueue() {
        return QueueBuilder.durable(NOTIFICATION_QUEUE).build();
    }

    @Bean
    public TopicExchange notificationExchange() {
        return new TopicExchange(NOTIFICATION_EXCHANGE);
    }

    @Bean
    public Binding notificationBinding(Queue notificationQueue, TopicExchange notificationExchange) {
        return BindingBuilder.bind(notificationQueue).to(notificationExchange).with(NOTIFICATION_KEY);
    }

    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }
}
