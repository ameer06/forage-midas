package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DatabaseConduit databaseConduit;

    public KafkaConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(String message) {
        try {
            Transaction transaction =
                    objectMapper.readValue(message, Transaction.class);

            if (transaction == null) {
                log.warn("Received null transaction from Kafka topic");
                return;
            }

            log.info("Received transaction - Amount: {}", transaction.getAmount());
            databaseConduit.processTransaction(transaction);

        } catch (Exception e) {
            log.error("Failed to process Kafka message: {}", message, e);
        }
    }
}