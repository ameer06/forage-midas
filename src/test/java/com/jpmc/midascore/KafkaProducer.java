package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(
            @Value("${general.kafka-topic}") String topic,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        try {
            String[] transactionData = transactionLine.split(", ");

            Transaction transaction = new Transaction(
                    Long.parseLong(transactionData[0]),
                    Long.parseLong(transactionData[1]),
                    Float.parseFloat(transactionData[2])
            );

            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(transaction);

            kafkaTemplate.send(topic, json);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}