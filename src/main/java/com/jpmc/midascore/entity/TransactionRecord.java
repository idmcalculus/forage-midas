package com.jpmc.midascore.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "transactions")
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String originalTransactionId; // To store the ID from the Kafka Transaction object

    @ManyToOne(fetch = FetchType.LAZY) // LAZY is often a good default
    @JoinColumn(name = "sender_id", nullable = false)
    private UserRecord sender;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "recipient_id", nullable = false)
    private UserRecord recipient;

    @Column(nullable = false, precision = 19, scale = 4) // Precision and scale for financial data
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Column(nullable = false)
    private LocalDateTime transactionTimestamp; // When the transaction was processed/persisted

    // JPA requires a no-arg constructor
    public TransactionRecord() {
    }

    public TransactionRecord(String originalTransactionId, UserRecord sender, UserRecord recipient, BigDecimal amount, String currency, LocalDateTime transactionTimestamp) {
        this.originalTransactionId = originalTransactionId;
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
        this.currency = currency;
        this.transactionTimestamp = transactionTimestamp;
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOriginalTransactionId() {
        return originalTransactionId;
    }

    public void setOriginalTransactionId(String originalTransactionId) {
        this.originalTransactionId = originalTransactionId;
    }

    public UserRecord getSender() {
        return sender;
    }

    public void setSender(UserRecord sender) {
        this.sender = sender;
    }

    public UserRecord getRecipient() {
        return recipient;
    }

    public void setRecipient(UserRecord recipient) {
        this.recipient = recipient;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public LocalDateTime getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public void setTransactionTimestamp(LocalDateTime transactionTimestamp) {
        this.transactionTimestamp = transactionTimestamp;
    }

    @Override
    public String toString() {
        return "TransactionRecord{" +
                "id=" + id +
                ", originalTransactionId='" + originalTransactionId + '\'' +
                ", senderId=" + (sender != null ? sender.getId() : null) +
                ", recipientId=" + (recipient != null ? recipient.getId() : null) +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", transactionTimestamp=" + transactionTimestamp +
                '}';
    }
}
