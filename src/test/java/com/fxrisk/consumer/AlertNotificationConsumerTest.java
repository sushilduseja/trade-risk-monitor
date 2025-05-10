package com.fxrisk.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertNotificationConsumerTest {

    private static final String TOPIC = "fx-risk-alerts";
    private static final String GROUP_ID = "test-group";
    
    private AlertNotificationConsumer consumer;
    
    @Mock
    private Consumer<String, String> mockKafkaConsumer;
    
    private MockConsumer<String, String> mockConsumer;
    private ExecutorService executor;
    private AtomicBoolean processedRecord;

    @BeforeEach
    void setUp() {
        processedRecord = new AtomicBoolean(false);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    @Test
    void shouldProcessAndTrackRecords() {
        // Arrange
        TestAlertNotificationConsumer testConsumer = new TestAlertNotificationConsumer(GROUP_ID);
        AlertNotificationConsumer consumer = spy(testConsumer);
        
        // Mock processing of a record
        doAnswer(invocation -> {
            String tradeId = invocation.getArgument(0);
            String message = invocation.getArgument(1);
            
            // Verify arguments
            assertEquals("trade-1", tradeId);
            assertEquals("test alert message", message);
            
            processedRecord.set(true);
            return CompletableFuture.completedFuture(null);
        }).when(consumer).processRecord(eq("trade-1"), eq("test alert message"));
        
        // Setup mock consumer with the test record
        MockConsumer<String, String> testMockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        List<TopicPartition> partitions = List.of(partition);
        
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(partition, 0L);
        
        testMockConsumer.assign(partitions);
        testMockConsumer.updateBeginningOffsets(beginningOffsets);
        
        // Add a record for the consumer to process
        testMockConsumer.addRecord(new ConsumerRecord<>(
            TOPIC, 0, 0, "trade-1", "test alert message"));
        
        // Override the createConsumer to return our configured mock 
        doReturn(testMockConsumer).when(consumer).createConsumer(anyString());
        
        // Execute in separate thread as run() is blocking
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                // Run the consumer until we process a record
                consumer.run();
            } catch (Exception e) {
                fail("Exception occurred during test: " + e.getMessage());
            }
        }, executor);
        
        // Wait for processing to complete
        await().atMost(5, TimeUnit.SECONDS).until(() -> processedRecord.get());
        
        // Stop the consumer
        consumer.shutdown();
        
        // Verify
        verify(consumer, times(1)).processRecord(eq("trade-1"), eq("test alert message"));
        assertTrue(processedRecord.get(), "Record should have been processed");
    }

    @Test
    void shouldHandleWakeupExceptionDuringShutdown() {
        // Arrange
        doThrow(new WakeupException()).when(mockKafkaConsumer).poll(any(Duration.class));
        
        consumer = new AlertNotificationConsumer(GROUP_ID) {
            @Override
            protected Consumer<String, String> createConsumer(String consumerGroupId) {
                return mockKafkaConsumer;
            }
        };
        
        // Act
        consumer.shutdown();
        consumer.run();
        
        // Assert
        verify(mockKafkaConsumer).wakeup();
        verify(mockKafkaConsumer).close();
    }

    @Test
    void shouldHandleExceptionDuringProcessing() {
        // Arrange
        consumer = spy(new AlertNotificationConsumer(GROUP_ID) {
            @Override
            protected Consumer<String, String> createConsumer(String consumerGroupId) {
                return mockKafkaConsumer;
            }
            
            @Override
            protected CompletableFuture<Void> processRecord(String tradeId, String message) {
                throw new RuntimeException("Test exception");
            }
        });
        
        // Setup mock to throw once then shutdown
        AtomicBoolean firstCall = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (firstCall.getAndSet(false)) {
                // First call - return a record to process
                HashMap<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
                records.put(
                    new TopicPartition(TOPIC, 0), 
                    List.of(new ConsumerRecord<>(TOPIC, 0, 0, "trade-1", "test message"))
                );
                return new ConsumerRecords<>(records);
            } else {
                // Second call - shutdown
                consumer.shutdown();
                throw new WakeupException();
            }
        }).when(mockKafkaConsumer).poll(any(Duration.class));
        
        // Act
        consumer.run();
        
        // Assert - should not throw, should handle the exception gracefully
        verify(mockKafkaConsumer, atLeastOnce()).poll(any(Duration.class));
        verify(consumer).processRecord(anyString(), anyString());
    }
    
    @Test
    void shouldWaitForInFlightNotificationsBeforeShutdown() throws Exception {
        // Arrange
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        consumer = spy(new AlertNotificationConsumer(GROUP_ID) {
            @Override
            protected Consumer<String, String> createConsumer(String consumerGroupId) {
                return mockKafkaConsumer;
            }
            
            @Override
            protected void waitForInFlightNotifications() {
                // Override to test the method directly
                super.waitForInFlightNotifications();
            }
        });
        
        // Add in-flight notification
        consumer.inFlightNotifications.put("trade-1", future);
        
        // Start shutdown in another thread
        Thread shutdownThread = new Thread(() -> {
            try {
                // Call waitForInFlightNotifications directly rather than through shutdown
                consumer.waitForInFlightNotifications();
            } catch (Exception e) {
                fail("Exception during waitForInFlightNotifications: " + e.getMessage());
            }
        });
        shutdownThread.start();
        
        // Wait a bit to ensure the method has started waiting
        Thread.sleep(100);
        
        // Complete the future
        future.complete(null);
        
        // Wait for the method to complete
        shutdownThread.join(1000);
        
        // Assert
        assertTrue(consumer.inFlightNotifications.isEmpty(), "In-flight notifications should be empty after waiting");
    }
    
    @Test
    void shouldTrackProcessingMetrics() {
        // Arrange
        consumer = new AlertNotificationConsumer(GROUP_ID) {
            @Override
            protected Consumer<String, String> createConsumer(String consumerGroupId) {
                return mockKafkaConsumer;
            }
            
            @Override
            protected void sendNotification(String tradeId, String message) {
                // Override to avoid actual sending
            }
        };
        
        // Act
        consumer.processRecord("trade-1", "test message");
        
        // Assert
        assertEquals(1, consumer.processedMessageCount.get(), "Should track processed message count");
    }
    
    // Helper class for testing
    static class TestAlertNotificationConsumer extends AlertNotificationConsumer {
        public TestAlertNotificationConsumer(String groupId) {
            super(groupId);
        }
        
        @Override
        protected Consumer<String, String> createConsumer(String consumerGroupId) {
            return new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        }
        
        @Override
        protected void sendNotification(String tradeId, String message) {
            // Override to avoid actual sending
        }
    }
    
    // Custom Awaitility-like helper for tests
    private interface Awaiter {
        Awaiter atMost(long amount, TimeUnit unit);
        void until(BooleanSupplier supplier);
    }
    
    private static Awaiter await() {
        return new Awaiter() {
            private long timeoutMs = 5000; // Default 5 seconds
            
            @Override
            public Awaiter atMost(long amount, TimeUnit unit) {
                this.timeoutMs = unit.toMillis(amount);
                return this;
            }
            
            @Override
            public void until(BooleanSupplier supplier) {
                long startTime = System.currentTimeMillis();
                while (!supplier.getAsBoolean()) {
                    if (System.currentTimeMillis() - startTime > timeoutMs) {
                        fail("Condition not met within timeout of " + timeoutMs + "ms");
                    }
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        fail("Interrupted while waiting for condition");
                    }
                }
            }
        };
    }
    
    // Functional interface for suppliers
    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
