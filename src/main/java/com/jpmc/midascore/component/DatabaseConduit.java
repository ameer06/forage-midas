package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class DatabaseConduit {

    private static final Logger log = LoggerFactory.getLogger(DatabaseConduit.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplateBuilder builder) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = builder.build();
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public UserRecord findById(long id) {
        return userRepository.findById(id);
    }

    public boolean processTransaction(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Validate: both users must exist
        if (sender == null || recipient == null) {
            log.warn("Invalid transaction: sender or recipient not found");
            return false;
        }

        // Validate: amount must be positive
        if (transaction.getAmount() <= 0) {
            log.warn("Invalid transaction: amount must be positive");
            return false;
        }

        // Validate: sender must have sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            log.warn("Invalid transaction: insufficient balance for sender {}", sender.getName());
            return false;
        }

        // Fetch incentive
        Incentive incentiveObj = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
        float incentive = incentiveObj != null ? incentiveObj.getAmount() : 0f;

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive);
        userRepository.save(sender);
        userRepository.save(recipient);

        // Persist the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentive);
        transactionRepository.save(record);

        log.info("Processed transaction: {} -> {}, amount: {}, incentive: {}",
                sender.getName(), recipient.getName(), transaction.getAmount(), incentive);
        return true;
    }
}
