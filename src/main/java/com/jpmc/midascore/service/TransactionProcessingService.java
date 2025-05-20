package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction; // Kafka DTO
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Service
public class TransactionProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessingService.class);

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    @Autowired
    public TransactionProcessingService(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @Transactional
    public void processTransaction(Transaction kafkaTransaction) {
        logger.info("Processing transaction: {}", kafkaTransaction.toString());

        long senderUserId = kafkaTransaction.getSenderId();
        long recipientUserId = kafkaTransaction.getRecipientId();
        float transactionAmountFloat = kafkaTransaction.getAmount();

        BigDecimal transactionAmountBigDecimal;
        try {
            transactionAmountBigDecimal = new BigDecimal(Float.toString(transactionAmountFloat));
        } catch (NumberFormatException e) {
            logger.error("Invalid amount format in Kafka transaction {}: {}. Discarding transaction.", kafkaTransaction.toString(), transactionAmountFloat);
            return;
        }

        UserRecord sender = userRepository.findById(senderUserId);
        if (sender == null) {
            logger.warn("Sender ID {} not found for transaction {}. Discarding transaction.", senderUserId, kafkaTransaction.toString());
            return;
        }

        UserRecord recipient = userRepository.findById(recipientUserId);
        if (recipient == null) {
            logger.warn("Recipient ID {} not found for transaction {}. Discarding transaction.", recipientUserId, kafkaTransaction.toString());
            return;
        }

        BigDecimal senderBalanceBigDecimal = new BigDecimal(Float.toString(sender.getBalance()));

        if (senderBalanceBigDecimal.compareTo(transactionAmountBigDecimal) < 0) {
            logger.warn("Insufficient balance for sender {} in transaction {}. Balance: {}, Amount: {}. Discarding transaction.",
                    senderUserId, kafkaTransaction.toString(), sender.getBalance(), transactionAmountFloat);
            return;
        }

        try {
            BigDecimal newSenderBalance = senderBalanceBigDecimal.subtract(transactionAmountBigDecimal);
            BigDecimal recipientBalanceBigDecimal = new BigDecimal(Float.toString(recipient.getBalance()));
            BigDecimal newRecipientBalance = recipientBalanceBigDecimal.add(transactionAmountBigDecimal);

            sender.setBalance(newSenderBalance.floatValue());
            recipient.setBalance(newRecipientBalance.floatValue());

            userRepository.save(sender);
            userRepository.save(recipient);

            String originalKafkaTxId = "kafkaTx-" + kafkaTransaction.getSenderId() + "-" + kafkaTransaction.getRecipientId() + "-" + System.nanoTime();

            // ASSUMPTION: Transaction DTO does not provide currency. Defaulting to "USD".
            String currency = "USD";
            logger.debug("Transaction DTO does not provide currency. Defaulting to '{}' for transaction: {}", currency, kafkaTransaction.toString());

            TransactionRecord transactionRecord = new TransactionRecord(
                    originalKafkaTxId,
                    sender,
                    recipient,
                    transactionAmountBigDecimal,
                    currency,
                    LocalDateTime.now()
            );
            transactionRecordRepository.save(transactionRecord);

            logger.info("Successfully processed and recorded transaction {}. Sender {} new balance: {}, Recipient {} new balance: {}",
                    kafkaTransaction.toString(), senderUserId, sender.getBalance(), recipientUserId, recipient.getBalance());

        } catch (Exception e) {
            logger.error("Error during database update for transaction {}. Transaction will be rolled back.", kafkaTransaction.toString(), e);
            throw e;
        }
    }
}
