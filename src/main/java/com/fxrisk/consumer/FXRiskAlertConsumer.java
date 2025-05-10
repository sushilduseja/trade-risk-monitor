package com.fxrisk.consumer;

import com.fxrisk.config.KafkaConfig;
import com.fxrisk.model.FXTrade;
import com.fxrisk.model.RiskLevel;
import com.fxrisk.serialization.JsonSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FXRiskAlertConsumer implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(FXRiskAlertConsumer.class);
    private static final String DLQ_TOPIC = "fx-trades-dlq";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration ERROR_RETRY_DELAY = Duration.ofSeconds(1);
    
    protected final Consumer<String, String> consumer;
    protected final Producer<String, String> alertProducer;
    protected final Producer<String, String> dlqProducer;
    private final JsonSerializer<FXTrade> serializer;
    private final AtomicBoolean running;
    
    // Performance monitoring metrics
    private final AtomicLong processedTradeCount = new AtomicLong(0);
    private final AtomicLong highRiskTradeCount = new AtomicLong(0);
    private final AtomicLong failedTradeCount = new AtomicLong(0);
    
    public FXRiskAlertConsumer() {
        this(
            new KafkaConsumer<>(KafkaConfig.getConsumerProperties("fx-risk-monitor")),
            new KafkaProducer<>(KafkaConfig.getProducerProperties()),
            new KafkaProducer<>(KafkaConfig.getProducerProperties())
        );
    }
    
    /**
     * Constructor that accepts a consumer group ID and alert topic
     * 
     * @param consumerGroupId The consumer group ID to use
     * @param alertTopic The topic to publish alerts to (usually KafkaConfig.FX_RISK_ALERTS_TOPIC)
     */
    public FXRiskAlertConsumer(String consumerGroupId, String alertTopic) {
        this(
            new KafkaConsumer<>(KafkaConfig.getConsumerProperties(consumerGroupId)),
            new KafkaProducer<>(KafkaConfig.getProducerProperties()),
            new KafkaProducer<>(KafkaConfig.getProducerProperties())
        );
    }
    
    /**
     * Constructor with a single String parameter for consumer group ID
     * 
     * @param consumerGroupId The consumer group ID to use
     */
    public FXRiskAlertConsumer(String consumerGroupId) {
        this(
            new KafkaConsumer<>(KafkaConfig.getConsumerProperties(consumerGroupId)),
            new KafkaProducer<>(KafkaConfig.getProducerProperties()),
            new KafkaProducer<>(KafkaConfig.getProducerProperties())
        );
    }
    
    public FXRiskAlertConsumer(
            Consumer<String, String> consumer,
            Producer<String, String> alertProducer,
            Producer<String, String> dlqProducer
    ) {
        this.consumer = consumer;
        this.alertProducer = alertProducer;
        this.dlqProducer = dlqProducer;
        this.serializer = new JsonSerializer<>();
        this.running = new AtomicBoolean(true);
    }
    
    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(KafkaConfig.FX_TRADES_TOPIC));
            logger.info("FX Risk Alert Consumer started, subscribed to: {}", KafkaConfig.FX_TRADES_TOPIC);
            
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                    
                    if (!records.isEmpty()) {
                        processTradeRecords(records);
                        commitOffsets();
                    }
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (!running.get()) {
                        break;
                    }
                    throw e;
                } catch (KafkaException ke) {
                    logger.error("Kafka error while processing records", ke);
                    sleepOnError();
                } catch (Exception e) {
                    logger.error("Unexpected error processing records", e);
                    sleepOnError();
                }
            }
        } finally {
            closeResources();
        }
    }
    
    private void processTradeRecords(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            processTradeRecord(record);
        }
    }
    
    private void commitOffsets() {
        try {
            consumer.commitSync();
        } catch (Exception e) {
            logger.error("Failed to commit offsets", e);
        }
    }
    
    private void sleepOnError() {
        try {
            Thread.sleep(ERROR_RETRY_DELAY.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
    
    protected void processTradeRecord(ConsumerRecord<String, String> record) {
        String tradeId = record.key();
        String tradeJson = record.value();
        
        if (tradeId == null || tradeId.isEmpty()) {
            logger.warn("Received trade record with null or empty key, using offset as ID");
            tradeId = "unknown-" + record.partition() + "-" + record.offset();
        }
        
        logger.info("Processing trade record: {}", tradeId);
        
        try {
            FXTrade trade = deserializeTradeRecord(tradeJson);
            RiskLevel riskLevel = trade.calculateRiskScore();
            
            processedTradeCount.incrementAndGet();
            
            if (riskLevel == RiskLevel.HIGH) {
                highRiskTradeCount.incrementAndGet();
                sendRiskAlert(tradeId, trade, riskLevel);
            } else {
                logger.info("Trade {} has {} risk level. No alert needed.", tradeId, riskLevel);
            }
        } catch (Exception e) {
            logger.error("Failed to process trade record: {}", tradeId, e);
            failedTradeCount.incrementAndGet();
            sendToDlq(tradeId, tradeJson);
        }
    }
    
    protected FXTrade deserializeTradeRecord(String tradeJson) {
        if (tradeJson == null || tradeJson.isEmpty()) {
            throw new IllegalArgumentException("Trade JSON data is null or empty");
        }
        
        try {
            return serializer.deserialize(tradeJson, FXTrade.class);
        } catch (Exception e) {
            logger.error("Failed to deserialize trade record: {}", e.getMessage());
            throw e;
        }
    }
    
    protected CompletableFuture<Void> sendRiskAlert(String tradeId, FXTrade trade, RiskLevel riskLevel) {
        String alertMessage = String.format(
            "HIGH RISK ALERT: Trade %s, %s %s %s with %s has high risk score",
            tradeId,
            trade.getDirection(),
            trade.getAmount(),
            trade.getCurrencyPair(),
            trade.getCounterparty()
        );
        
        logger.info("Sending risk alert: {}", alertMessage);
        
        ProducerRecord<String, String> alertRecord = new ProducerRecord<>(
            KafkaConfig.FX_RISK_ALERTS_TOPIC,
            tradeId,
            alertMessage
        );
        
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        alertProducer.send(alertRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send risk alert for trade {}", tradeId, exception);
                future.completeExceptionally(exception);
            } else {
                logger.info("Risk alert sent for trade {}", tradeId);
                future.complete(null);
            }
        });
        
        return future;
    }
    
    protected void sendToDlq(String tradeId, String originalMessage) {
        logger.info("Sending message to DLQ: {}", tradeId);
        
        // Add error context to the message
        String dlqMessage = String.format(
            "{\"original_message\":%s,\"error_context\":{\"timestamp\":\"%s\",\"reason\":\"Processing failure\"}}",
            originalMessage,
            java.time.Instant.now()
        );
        
        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
            DLQ_TOPIC,
            tradeId,
            dlqMessage
        );
        
        dlqProducer.send(dlqRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send message to DLQ for trade {}", tradeId, exception);
            } else {
                logger.info("Message sent to DLQ for trade {}", tradeId);
            }
        });
    }
    
    private void closeResources() {
        logger.info("Closing FX Risk Alert Consumer - processed: {}, high risk: {}, failed: {}", 
            processedTradeCount.get(), highRiskTradeCount.get(), failedTradeCount.get());
            
        try {
            consumer.close();
            logger.info("Consumer closed");
        } catch (Exception e) {
            logger.error("Error closing consumer", e);
        }
        
        try {
            alertProducer.close();
            dlqProducer.close();
            logger.info("Producers closed");
        } catch (Exception e) {
            logger.error("Error closing producers", e);
        }
    }
    
    public void shutdown() {
        logger.info("Shutting down FX Risk Alert Consumer...");
        running.set(false);
        consumer.wakeup();
    }
    
    @Override
    public void close() {
        shutdown();
    }
    
    // For metrics/monitoring
    public long getProcessedTradeCount() {
        return processedTradeCount.get();
    }
    
    public long getHighRiskTradeCount() {
        return highRiskTradeCount.get();
    }
    
    public long getFailedTradeCount() {
        return failedTradeCount.get();
    }
    
    /**
     * Process a single trade record with the given ID and JSON payload.
     * This method is primarily intended for testing.
     * 
     * @param tradeId The ID of the trade to process
     * @param tradeJson The JSON representation of the trade
     */
    public void processRecord(String tradeId, String tradeJson) {
        try {
            FXTrade trade = deserializeTradeRecord(tradeJson);
            RiskLevel riskLevel = trade.calculateRiskScore();
            
            processedTradeCount.incrementAndGet();
            
            if (riskLevel == RiskLevel.HIGH) {
                highRiskTradeCount.incrementAndGet();
                sendRiskAlert(tradeId, trade, riskLevel);
            } else {
                logger.info("Trade {} has {} risk level. No alert needed.", tradeId, riskLevel);
            }
        } catch (Exception e) {
            logger.error("Failed to process trade record: {}", tradeId, e);
            failedTradeCount.incrementAndGet();
            sendToDlq(tradeId, tradeJson);
        }
    }
}
