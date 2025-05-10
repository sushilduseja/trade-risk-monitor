package com.fxrisk.app;

import com.fxrisk.config.KafkaConfig;
import com.fxrisk.consumer.AlertNotificationConsumer;
import com.fxrisk.consumer.FXRiskAlertConsumer;
import com.fxrisk.model.FXTrade;
import com.fxrisk.producer.FXTradeProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main application class for the FX Risk Monitoring system.
 * Starts and manages the various components of the system.
 */
public class FXRiskMonitoringApp {
    private static final Logger logger = LoggerFactory.getLogger(FXRiskMonitoringApp.class);
    
    private static final String RISK_CONSUMER_GROUP = "fx-risk-monitor";
    private static final String NOTIFICATION_CONSUMER_GROUP = "fx-notification-service";

    public static void main(String[] args) {
        logger.info("Starting FX Risk Monitoring Application");
        
        try {
            // Ensure Kafka topics exist - this will now handle Kafka connection issues gracefully
            KafkaConfig.ensureTopicsExist();
            
            // Start alert notification consumer in a separate thread
            Thread notificationThread = new Thread(() -> {
                try (AlertNotificationConsumer notificationConsumer = 
                        new AlertNotificationConsumer(NOTIFICATION_CONSUMER_GROUP)) {
                    notificationConsumer.run();
                } catch (Exception e) {
                    logger.error("Error in notification consumer thread", e);
                }
            });
            notificationThread.setName("notification-consumer");
            notificationThread.setDaemon(true); // Make thread a daemon so it doesn't prevent JVM shutdown
            notificationThread.start();
            
            // Start risk alert consumer in a separate thread
            Thread riskAlertThread = new Thread(() -> {
                try (FXRiskAlertConsumer riskConsumer = 
                        new FXRiskAlertConsumer(RISK_CONSUMER_GROUP, KafkaConfig.FX_RISK_ALERTS_TOPIC)) {
                    riskConsumer.run();
                } catch (Exception e) {
                    logger.error("Error in risk alert consumer thread", e);
                }
            });
            riskAlertThread.setName("risk-alert-consumer");
            riskAlertThread.setDaemon(true); // Make thread a daemon so it doesn't prevent JVM shutdown
            riskAlertThread.start();
            
            // Generate sample trades for demonstration - wrap in try/catch to handle Kafka connection issues
            try {
                logger.info("Generating sample trades...");
                generateSampleTrades();
            } catch (Exception e) {
                logger.error("Failed to generate sample trades", e);
                logger.warn("Continuing application execution despite trade generation failure");
            }
            
            // Keep application running
            try {
                logger.info("Application started successfully, running for 5 minutes...");
                Thread.sleep(Duration.ofMinutes(5).toMillis());
                
                // Try to generate high volume to test performance
                try {
                    generateHighVolumeTrades(1000, 100);
                } catch (Exception e) {
                    logger.error("Failed to generate high volume trades", e);
                }
                
                // Try to test partition rebalancing
                try {
                    testPartitionRebalance();
                } catch (Exception e) {
                    logger.error("Failed to test partition rebalancing", e);
                }
                
            } catch (InterruptedException e) {
                logger.error("Application interrupted", e);
                Thread.currentThread().interrupt();
            }
            
        } catch (Exception e) {
            logger.error("Fatal error in application startup", e);
        }
        
        logger.info("FX Risk Monitoring Application shutdown complete");
    }
    
    /**
     * Tests partition rebalancing by starting and stopping consumers during processing.
     * This helps ensure the system remains resilient during rebalances.
     */
    private static void testPartitionRebalance() {
        logger.info("Testing partition rebalancing...");
        
        List<Thread> consumerThreads = new ArrayList<>();
        List<FXRiskAlertConsumer> consumers = new ArrayList<>();
        
        // Start 3 additional consumers
        for (int i = 0; i < 3; i++) {
            FXRiskAlertConsumer consumer = new FXRiskAlertConsumer(RISK_CONSUMER_GROUP + "-rebalance", KafkaConfig.FX_RISK_ALERTS_TOPIC);
            consumers.add(consumer);
            
            Thread thread = new Thread(consumer);
            thread.setName("rebalance-consumer-" + i);
            thread.start();
            consumerThreads.add(thread);
            
            try {
                // Wait a bit before starting the next consumer
                Thread.sleep(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Generate some trades during rebalancing
        generateHighVolumeTrades(500, 50);
        
        // Stop consumers one by one
        for (int i = 0; i < consumers.size(); i++) {
            try {
                Thread.sleep(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            logger.info("Stopping consumer {}", i);
            consumers.get(i).shutdown();
        }
        
        // Wait for all threads to complete
        for (Thread thread : consumerThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("Partition rebalancing test completed");
    }
    
    /**
     * Generates a high volume of trades to test system performance.
     * 
     * @param count The number of trades to generate
     * @param batchSize The number of trades to publish in a single batch
     */
    private static void generateHighVolumeTrades(int count, int batchSize) {
        logger.info("Generating {} trades with batch size {}", count, batchSize);
        
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CompletableFuture<?>[] futures = new CompletableFuture[count / batchSize];
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (int i = 0; i < count / batchSize; i++) {
            final int batchIndex = i;
            futures[i] = CompletableFuture.runAsync(() -> {
                try (FXTradeProducer producer = new FXTradeProducer()) {
                    for (int j = 0; j < batchSize; j++) {
                        int tradeId = batchIndex * batchSize + j;
                        FXTrade trade = generateRandomTrade(tradeId);
                        
                        CompletableFuture<RecordMetadata> future = producer.publishTrade(trade);
                        future.thenAccept(metadata -> successCount.incrementAndGet());
                    }
                }
            }, executor);
        }
        
        // Wait for all batches to complete
        CompletableFuture.allOf(futures).join();
        
        logger.info("Successfully generated {} trades", successCount.get());
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Generates sample trades for demonstration purposes.
     */
    private static void generateSampleTrades() {
        logger.info("Generating sample trades...");
        
        try (FXTradeProducer producer = new FXTradeProducer()) {
            // Generate a regular trade
            FXTrade regularTrade = new FXTrade();
            regularTrade.setTradeId("T-REGULAR-001");
            regularTrade.setBaseCurrency("USD");
            regularTrade.setCounterCurrency("EUR");
            regularTrade.setAmount(new BigDecimal("1000000.00"));
            regularTrade.setRate(new BigDecimal("0.85"));
            regularTrade.setTrader("John Doe");
            regularTrade.setTradeDate("2023-06-15");
            regularTrade.setValueDate("2023-06-17");
            
            producer.publishTrade(regularTrade);
            logger.info("Sent regular trade: {}", regularTrade.getTradeId());
            
            // Generate a high-value trade that should trigger alerts
            FXTrade highValueTrade = new FXTrade();
            highValueTrade.setTradeId("T-HIGHVAL-001");
            highValueTrade.setBaseCurrency("USD");
            highValueTrade.setCounterCurrency("JPY");
            highValueTrade.setAmount(new BigDecimal("50000000.00"));  // 50M USD
            highValueTrade.setRate(new BigDecimal("110.25"));
            highValueTrade.setTrader("Jane Smith");
            highValueTrade.setTradeDate("2023-06-15");
            highValueTrade.setValueDate("2023-06-17");
            
            producer.publishTrade(highValueTrade);
            logger.info("Sent high-value trade: {}", highValueTrade.getTradeId());
            
            // Generate an unusual currency pair trade
            FXTrade unusualPairTrade = new FXTrade();
            unusualPairTrade.setTradeId("T-UNUSUAL-001");
            unusualPairTrade.setBaseCurrency("MXN");
            unusualPairTrade.setCounterCurrency("ZAR");
            unusualPairTrade.setAmount(new BigDecimal("5000000.00"));
            unusualPairTrade.setRate(new BigDecimal("1.25"));
            unusualPairTrade.setTrader("Carlos Rodriguez");
            unusualPairTrade.setTradeDate("2023-06-15");
            unusualPairTrade.setValueDate("2023-06-17");
            
            producer.publishTrade(unusualPairTrade);
            logger.info("Sent unusual currency pair trade: {}", unusualPairTrade.getTradeId());
            
            // Generate a trade with unusual rate
            FXTrade unusualRateTrade = new FXTrade();
            unusualRateTrade.setTradeId("T-RATE-001");
            unusualRateTrade.setBaseCurrency("EUR");
            unusualRateTrade.setCounterCurrency("USD");
            unusualRateTrade.setAmount(new BigDecimal("2000000.00"));
            unusualRateTrade.setRate(new BigDecimal("0.75"));  // Unusual rate for EUR/USD
            unusualRateTrade.setTrader("Marie Dupont");
            unusualRateTrade.setTradeDate("2023-06-15");
            unusualRateTrade.setValueDate("2023-06-17");
            
            producer.publishTrade(unusualRateTrade);
            logger.info("Sent unusual rate trade: {}", unusualRateTrade.getTradeId());
        }
        
        logger.info("Sample trades generation completed");
    }
    
    /**
     * Generates a random trade for testing purposes.
     * 
     * @param id An identifier to make the trade unique
     * @return A randomly generated FXTrade
     */
    private static FXTrade generateRandomTrade(int id) {
        Random random = new Random();
        String[] currencies = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"};
        String[] traders = {"John Smith", "Jane Doe", "Robert Johnson", "Emma Williams", "Michael Brown"};
        
        FXTrade trade = new FXTrade();
        trade.setTradeId("T-" + id);
        
        // Select base and counter currencies ensuring they're different
        int baseIndex = random.nextInt(currencies.length);
        int counterIndex;
        do {
            counterIndex = random.nextInt(currencies.length);
        } while (counterIndex == baseIndex);
        
        trade.setBaseCurrency(currencies[baseIndex]);
        trade.setCounterCurrency(currencies[counterIndex]);
        
        // Generate a random amount between 100,000 and 10,000,000
        BigDecimal amount = BigDecimal.valueOf(100000 + random.nextInt(9900000));
        trade.setAmount(amount.setScale(2, RoundingMode.HALF_UP));
        
        // Generate a random rate between 0.5 and 2.0
        BigDecimal rate = BigDecimal.valueOf(0.5 + random.nextDouble() * 1.5);
        trade.setRate(rate.setScale(4, RoundingMode.HALF_UP));
        
        // Pick a random trader
        trade.setTrader(traders[random.nextInt(traders.length)]);
        
        // Set today as trade date
        trade.setTradeDate("2023-06-15");
        trade.setValueDate("2023-06-17");
        
        return trade;
    }
}