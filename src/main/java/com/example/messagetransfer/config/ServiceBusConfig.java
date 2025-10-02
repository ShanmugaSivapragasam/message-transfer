package com.example.messagetransfer.config;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceBusConfig {

    @Value("${azure.servicebus.source.connection-string}")
    private String sourceConnectionString;
    @Value("${azure.servicebus.source.queue}")
    private String sourceQueue;

    @Value("${azure.servicebus.dest.connection-string}")
    private String destConnectionString;
    @Value("${azure.servicebus.dest.queue}")
    private String destQueue;

    @Value("${azure.servicebus.error.queue:poc-dead-letter}")
    private String errorQueue;

    @Bean
    public ServiceBusSenderClient sourceSender() {
        return new ServiceBusClientBuilder()
                .connectionString(sourceConnectionString)
                .sender()
                .queueName(sourceQueue)
                .buildClient();
    }

    @Bean
    public ServiceBusSenderClient destSender() {
        return new ServiceBusClientBuilder()
                .connectionString(destConnectionString)
                .sender()
                .queueName(destQueue)
                .buildClient();
    }

    @Bean
    public ServiceBusSenderClient errorSender() {
        return new ServiceBusClientBuilder()
                .connectionString(destConnectionString)
                .sender()
                .queueName(errorQueue)
                .buildClient();
    }

    @Bean
    public ServiceBusReceiverClient sourcePeeker() {
        return new ServiceBusClientBuilder()
                .connectionString(sourceConnectionString)
                .receiver()
                .queueName(sourceQueue)
                .disableAutoComplete()
                .buildClient();
    }

    @Bean
    public ServiceBusReceiverClient destPeeker() {
        return new ServiceBusClientBuilder()
                .connectionString(destConnectionString)
                .receiver()
                .queueName(destQueue)
                .disableAutoComplete()
                .buildClient();
    }

    @Bean
    public ServiceBusReceiverClient errorReceiver() {
        return new ServiceBusClientBuilder()
                .connectionString(destConnectionString)
                .receiver()
                .queueName(errorQueue)
                .disableAutoComplete()
                .buildClient();
    }
}
