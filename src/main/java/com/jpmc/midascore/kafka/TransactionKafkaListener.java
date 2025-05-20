package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction; 
import com.jpmc.midascore.service.TransactionProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class TransactionKafkaListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionKafkaListener.class);

    private final Environment environment;
    private final TransactionProcessingService transactionProcessingService;

    @Autowired
    public TransactionKafkaListener(Environment environment, TransactionProcessingService transactionProcessingService) {
        this.environment = environment;
        this.transactionProcessingService = transactionProcessingService;
    }

    @PostConstruct
    public void logTopic() {
        LOGGER.info("Listening to Kafka topic: {}", environment.getProperty("general.kafka-topic"));
    }

    @KafkaListener(topics = "#{environment.getProperty('general.kafka-topic')}", groupId = "${spring.kafka.consumer.group-id}")
    public void receiveTransaction(@Payload Transaction transaction) {
        LOGGER.info("Received transaction on topic '{}': {}", environment.getProperty("general.kafka-topic"), transaction);
        transactionProcessingService.processTransaction(transaction);
    }
}
