package com.fxrisk.consumer;

import com.fxrisk.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class AlertNotificationConsumer implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AlertNotificationConsumer.class);
    private final Consumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String consumerGroupId;
    
    // Use virtual threads executor for notification sending (Java 21 feature)
    private final Executor notificationExecutor = Executors.newVirtualThreadPerTaskExecutor();
    
    // Track in-flight notifications to ensure they complete before shutdown
    private final Map<String, CompletableFuture<Void>> inFlightNotifications = new ConcurrentHashMap<>();

    public AlertNotificationConsumer(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        this.consumer = new KafkaConsumer<>(KafkaConfig.getConsumerProperties(consumerGroupId));
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(KafkaConfig.FX_RISK_ALERTS_TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    logger.info("Partitions revoked: {}", partitions);
                    // Wait for all in-flight notifications to complete before allowing rebalance
                    waitForInFlightNotifications();
                    consumer.commitSync();
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    logger.info("Partitions assigned: {}", partitions);
                }
            });
            
            logger.info("Alert notification consumer started with group: {}", consumerGroupId);
            
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    
                    for (ConsumerRecord<String, String> record : records) {
                        String tradeId = record.key() != null ? record.key() : "unknown-trade";
                        
                        // Process this record and get a future that completes when notification is sent
                        CompletableFuture<Void> future = processRecord(tradeId, record.value());
                        
                        // Keep track of this in-flight notification
                        inFlightNotifications.put(tradeId, future);
                        
                        // When notification completes, remove from tracking and commit offset
                        future.whenComplete((result, ex) -> {
                            inFlightNotifications.remove(tradeId);
                            if (ex != null) {
                                logger.error("Notification failed for trade {}", tradeId, ex);
                            } else {
                                // Offset tracking happens in the main thread below
                                logger.debug("Notification completed for trade {}", tradeId);
                            }
                        });
                        
                        // Track offsets to commit - we'll commit after launching all notifications
                        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                        offsetsToCommit.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    }
                    
                    // Commit offsets for all records we've seen, without waiting for notifications to complete
                    // This approach allows for high throughput, but might result in duplicate notifications on rebalance
                    // For guaranteed exactly-once notification, you'd wait for all futures to complete before committing
                    if (!offsetsToCommit.isEmpty()) {
                        consumer.commitAsync(offsetsToCommit, (offsets, exception) -> {
                            if (exception != null) {
                                logger.error("Failed to commit offsets", exception);
                            }
                        });
                    }
                } catch (WakeupException e) {
                    // Special exception thrown from another thread to interrupt the consumer
                    if (!running.get()) {
                        // Break out of the loop if we're shutting down
                        break;
                    }
                    throw e; // Otherwise, re-throw to handle as a normal error
                } catch (Exception e) {
                    logger.error("Error in alert notification consumer polling loop", e);
                    // Short pause before retrying on unexpected errors
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            logger.error("Fatal error in alert notification consumer", e);
        } finally {
            // Wait for in-flight notifications to complete before closing
            waitForInFlightNotifications();
            try {
                consumer.close();
                logger.info("Consumer closed");
            } catch (Exception e) {
                logger.error("Error closing consumer", e);
            }
        }
    }
    
    private CompletableFuture<Void> processRecord(String tradeId, String message) {
        // Log notification from main thread
        logger.info("NOTIFICATION: {}", message);
        
        // Return a future that will complete when the notification is sent
        return CompletableFuture.runAsync(() -> sendNotification(tradeId, message), notificationExecutor);
    }
    
    private void sendNotification(String tradeId, String message) {
        // Simulate sending a notification with a slight delay
        try {
            // This would be replaced with actual notification logic in a real application
            logger.info("Sending external notification for trade {}: {}", tradeId, message);
            
            // Simulate external API call with different notification channels
            switch (message.hashCode() % 3) {
                case 0 -> {
                    logger.info("Sending email notification...");
                    Thread.sleep(500);  // Simulate email API
                }
                case 1 -> {
                    logger.info("Sending SMS notification...");
                    Thread.sleep(800);  // Simulate SMS API
                }
                default -> {
                    logger.info("Sending push notification...");
                    Thread.sleep(300);  // Simulate push notification
                }
            }
            
            logger.info("External notification sent successfully for trade {}", tradeId);
        } catch (InterruptedException e) {
            logger.error("Notification interrupted for trade {}", tradeId, e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Notification sending was interrupted", e);
        } catch (Exception e) {
            logger.error("Failed to send notification for trade {}", tradeId, e);
            throw new RuntimeException("Failed to send notification", e);
        }
    }
    
    private void waitForInFlightNotifications() {
        if (inFlightNotifications.isEmpty()) {
            return;
        }
        
        logger.info("Waiting for {} in-flight notifications to complete...", inFlightNotifications.size());
        
        try {
            // Create a combined future that completes when all in-flight notifications complete
            CompletableFuture<Void> allDone = CompletableFuture.allOf(
                inFlightNotifications.values().toArray(new CompletableFuture[0])
            );
            
            // Wait for all notifications to complete
            allDone.join();
            logger.info("All in-flight notifications completed");
            
        } catch (Exception e) {
            logger.error("Error waiting for in-flight notifications", e);
        } finally {
            inFlightNotifications.clear();
        }
    }

    public void shutdown() {
        logger.info("Shutting down alert notification consumer...");
        running.set(false);
        consumer.wakeup(); // This will throw WakeupException in consumer.poll()
    }

    @Override
    public void close() {
        shutdown();
    }
}
