package com.fxrisk.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fxrisk.config.KafkaConfig;
import com.fxrisk.model.FXTrade;
import com.fxrisk.util.JsonSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class FXRiskAlertConsumer implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(FXRiskAlertConsumer.class);
    private static final String DLQ_TOPIC = "fx-trades-dlq"; // Dead Letter Queue for failed messages
    private static final int MAX_RETRIES = 3;
    
    private final Consumer<String, String> consumer;
    private final Producer<String, String> alertProducer;
    private final Producer<String, String> dlqProducer; // Producer for Dead Letter Queue
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String consumerGroupId;
    private final Map<String, Integer> retryCount = new HashMap<>();

    public FXRiskAlertConsumer(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        this.consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties(consumerGroupId));
        this.alertProducer = new KafkaProducer<>(KafkaConfig.getProducerProperties());
        this.dlqProducer = new KafkaProducer<>(KafkaConfig.getProducerProperties());
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(KafkaConfig.FX_TRADES_TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // This is called before a rebalance starts and after the consumer stops fetching data
                    logger.info("Partitions revoked: {}", partitions);
                    // Commit offsets for the partitions we're about to lose
                    consumer.commitSync(); 
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // This is called after partitions have been reassigned to the consumer
                    logger.info("Partitions assigned: {}", partitions);
                }
            });
            
            logger.info("FX risk alert consumer started with group: {}", consumerGroupId);

            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    
                    for (ConsumerRecord<String, String> record : records) {
                        boolean processed = processFXTradeRecord(record);
                        
                        // If processed successfully, track the offset
                        if (processed) {
                            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                            offsetsToCommit.put(partition, new OffsetAndMetadata(record.offset() + 1));
                        }
                    }
                    
                    // Commit offsets for successfully processed records
                    if (!offsetsToCommit.isEmpty()) {
                        consumer.commitSync(offsetsToCommit);
                    }
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (running.get()) {
                        throw e;
                    }
                } catch (Exception e) {
                    logger.error("Error in FX risk alert consumer polling loop", e);
                    // Short pause before retrying on unexpected errors
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            logger.error("Fatal error in FX risk alert consumer", e);
        } finally {
            try {
                consumer.close();
                logger.info("Consumer closed");
            } catch (Exception e) {
                logger.error("Error closing consumer", e);
            }
        }
    }

    private boolean processFXTradeRecord(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();
        
        logger.debug("Processing FX trade record - Key: {}, Partition: {}, Offset: {}",
                key, record.partition(), record.offset());
        
        try {
            FXTrade trade = JsonSerializer.deserialize(value, FXTrade.class);
            
            // Check if trade is high risk based on risk score and threshold
            if (isHighRisk(trade)) {
                publishRiskAlert(trade);
            } else {
                logger.debug("Normal risk level for trade: {}", key);
            }
            
            // Reset retry count on success
            retryCount.remove(key);
            return true;
            
        } catch (RuntimeException e) {
            if (e.getCause() instanceof JsonProcessingException) {
                // Handle deserialization errors
                logger.error("Failed to deserialize trade data: {}", value, e);
                sendToDeadLetterQueue(key, value, "Deserialization error: " + e.getMessage());
                return true; // Consider it "processed" even though it failed (we're sending to DLQ)
            } else {
                // For other runtime errors, retry logic
                int attempts = retryCount.getOrDefault(key, 0) + 1;
                retryCount.put(key, attempts);
                
                if (attempts >= MAX_RETRIES) {
                    logger.error("Max retries reached for trade: {}. Sending to DLQ.", key);
                    sendToDeadLetterQueue(key, value, "Processing error after " + attempts + " retries: " + e.getMessage());
                    return true; // Mark as processed after sending to DLQ
                } else {
                    logger.warn("Processing error for trade: {}. Retry attempt {}/{}",
                            key, attempts, MAX_RETRIES, e);
                    return false; // Don't commit offset, will retry
                }
            }
        }
    }
    
    private boolean isHighRisk(FXTrade trade) {
        return trade.getRiskScore().compareTo(trade.getRiskThreshold()) > 0;
    }
    
    private void publishRiskAlert(FXTrade trade) {
        String alertMessage = String.format(
                "HIGH RISK ALERT: %s %s trade for %s %s with risk score %s (threshold: %s)",
                trade.getCurrencyPair(), 
                trade.getDirection(),
                trade.getAmount(),
                trade.getCurrencyPair(),
                trade.getRiskScore(), 
                trade.getRiskThreshold()
        );
        
        logger.info(alertMessage);
        
        // Publish to alerts topic
        try {
            alertProducer.send(
                new ProducerRecord<>(
                    KafkaConfig.FX_RISK_ALERTS_TOPIC,
                    trade.getTradeId(),
                    alertMessage
                )
            ).get(); // Wait for confirmation of delivery
            
            logger.debug("Alert published successfully for trade: {}", trade.getTradeId());
        } catch (Exception e) {
            logger.error("Failed to publish alert for trade: {}", trade.getTradeId(), e);
            // We don't retry here since this is an alert publishing failure,
            // but in a production system you might want different error handling
        }
    }
    
    private void sendToDeadLetterQueue(String key, String value, String errorMessage) {
        try {
            // Create a DLQ message with the original message and error details
            Map<String, Object> dlqMessage = new HashMap<>();
            dlqMessage.put("original_key", key);
            dlqMessage.put("original_value", value);
            dlqMessage.put("error", errorMessage);
            dlqMessage.put("timestamp", System.currentTimeMillis());
            dlqMessage.put("topic", KafkaConfig.FX_TRADES_TOPIC);
            
            String dlqValue = JsonSerializer.serialize(dlqMessage);
            
            // Send to DLQ topic
            dlqProducer.send(new ProducerRecord<>(DLQ_TOPIC, key, dlqValue)).get();
            logger.info("Message sent to DLQ: {}", key);
            
        } catch (Exception e) {
            logger.error("Failed to send message to DLQ: {}", key, e);
        }
    }

    public void shutdown() {
        logger.info("Shutting down FX risk alert consumer...");
        running.set(false);
        consumer.wakeup(); // Interrupt consumer.poll() to exit gracefully
    }

    @Override
    public void close() {
        shutdown();
        try {
            alertProducer.close();
            dlqProducer.close();
            logger.info("All producers closed");
        } catch (Exception e) {
            logger.error("Error closing producers", e);
        }
    }
}
