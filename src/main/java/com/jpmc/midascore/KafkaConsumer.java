package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(String message) {
        try {
            Transaction transaction =
                    objectMapper.readValue(message, Transaction.class);

            System.out.println("Amount: " + transaction.getAmount());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}