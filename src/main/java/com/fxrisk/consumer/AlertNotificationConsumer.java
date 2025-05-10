package com.fxrisk.consumer;

import com.fxrisk.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AlertNotificationConsumer implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AlertNotificationConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration ERROR_RETRY_DELAY = Duration.ofSeconds(1);
    private static final int METRICS_LOG_INTERVAL = 1000;
    
    protected final Consumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String consumerGroupId;
    
    // Use virtual threads executor for notification sending (Java 21 feature)
    private final Executor notificationExecutor;
    
    // Track in-flight notifications to ensure they complete before shutdown
    protected final Map<String, CompletableFuture<Void>> inFlightNotifications = new ConcurrentHashMap<>();
    
    // Performance monitoring metrics
    protected final AtomicLong processedMessageCount = new AtomicLong(0);
    protected final AtomicLong processingTime = new AtomicLong(0);
    protected final AtomicLong failedMessageCount = new AtomicLong(0);
    private volatile long startTime;

    public AlertNotificationConsumer(String consumerGroupId) {
        this(consumerGroupId, Executors.newVirtualThreadPerTaskExecutor());
    }
    
    // Constructor with dependency injection for easier testing
    public AlertNotificationConsumer(String consumerGroupId, Executor notificationExecutor) {
        this.consumerGroupId = consumerGroupId;
        this.notificationExecutor = notificationExecutor;
        this.consumer = createConsumer(consumerGroupId);
    }
    
    // Factory method that can be overridden in tests
    protected Consumer<String, String> createConsumer(String consumerGroupId) {
        return new KafkaConsumer<>(KafkaConfig.getConsumerProperties(consumerGroupId));
    }

    @Override
    public void run() {
        try {
            startTime = System.currentTimeMillis();
            consumer.subscribe(Collections.singletonList(KafkaConfig.FX_RISK_ALERTS_TOPIC), new RebalanceListener());
            
            logger.info("Alert notification consumer started with group: {}", consumerGroupId);
            
            processMessagesUntilShutdown();
        } catch (Exception e) {
            logger.error("Fatal error in alert notification consumer", e);
        } finally {
            cleanupResources();
        }
    }
    
    private void processMessagesUntilShutdown() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = processRecordsAndTrackOffsets(records);
                
                // Commit offsets for all records we've seen
                if (!offsetsToCommit.isEmpty()) {
                    commitOffsetsAsync(offsetsToCommit);
                    logPerformanceMetrics();
                }
            } catch (WakeupException e) {
                // Special exception thrown from another thread to interrupt the consumer
                if (!running.get()) {
                    break;
                }
                throw e; // Otherwise, re-throw to handle as a normal error
            } catch (Exception e) {
                logger.error("Error in alert notification consumer polling loop", e);
                sleepOnError();
            }
        }
    }
    
    private Map<TopicPartition, OffsetAndMetadata> processRecordsAndTrackOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        
        for (ConsumerRecord<String, String> record : records) {
            String tradeId = record.key() != null ? record.key() : "unknown-trade";
            
            // Process this record and get a future that completes when notification is sent
            CompletableFuture<Void> future = processRecord(tradeId, record.value());
            
            // Keep track of this in-flight notification
            inFlightNotifications.put(tradeId, future);
            
            // When notification completes, remove from tracking
            future.whenComplete((result, ex) -> {
                inFlightNotifications.remove(tradeId);
                if (ex != null) {
                    logger.error("Notification failed for trade {}", tradeId, ex);
                } else {
                    logger.debug("Notification completed for trade {}", tradeId);
                }
            });
            
            // Track offsets to commit - we'll commit after launching all notifications
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            offsetsToCommit.put(partition, new OffsetAndMetadata(record.offset() + 1));
        }
        
        return offsetsToCommit;
    }
    
    private void commitOffsetsAsync(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        consumer.commitAsync(offsetsToCommit, (offsets, exception) -> {
            if (exception != null) {
                logger.error("Failed to commit offsets", exception);
            }
        });
    }
    
    private void sleepOnError() {
        try {
            Thread.sleep(ERROR_RETRY_DELAY.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
    
    protected CompletableFuture<Void> processRecord(String tradeId, String message) {
        // Log notification from main thread
        logger.info("NOTIFICATION: {}", message);
        
        // Increment processed message count
        processedMessageCount.incrementAndGet();
        
        // Track processing time
        long startProcessingTime = System.currentTimeMillis();
        
        // Return a future that will complete when the notification is sent
        return CompletableFuture.runAsync(() -> sendNotification(tradeId, message), notificationExecutor)
            .whenComplete((result, ex) -> {
                // Record processing time
                long processingDuration = System.currentTimeMillis() - startProcessingTime;
                processingTime.addAndGet(processingDuration);
                
                // Track failures
                if (ex != null) {
                    failedMessageCount.incrementAndGet();
                }
            });
    }
    
    protected void sendNotification(String tradeId, String message) {
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
    
    protected void logPerformanceMetrics() {
        long count = processedMessageCount.get();
        if (count % METRICS_LOG_INTERVAL == 0 && count > 0) {
            long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
            if (elapsedSeconds > 0) {
                double avgProcessingTimeMs = processingTime.get() / (double)count;
                double throughput = count / (double)elapsedSeconds;
                
                logger.info("Performance: {} msgs/sec, avg processing: {} ms, in-flight: {}, failures: {}", 
                    String.format("%.2f", throughput), String.format("%.2f", avgProcessingTimeMs), 
                    inFlightNotifications.size(), failedMessageCount.get());
            }
        }
    }
    
    protected void waitForInFlightNotifications() {
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
    
    private void cleanupResources() {
        // Wait for in-flight notifications to complete before closing
        waitForInFlightNotifications();
        try {
            consumer.close();
            logger.info("Consumer closed");
        } catch (Exception e) {
            logger.error("Error closing consumer", e);
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
    
    // Inner class for rebalance listening to improve encapsulation
    private class RebalanceListener implements ConsumerRebalanceListener {
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
    }
}
