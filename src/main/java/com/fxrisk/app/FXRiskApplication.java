package com.fxrisk.app;

import com.fxrisk.consumer.AlertNotificationConsumer;
import com.fxrisk.consumer.FXRiskAlertConsumer;
import com.fxrisk.model.FXTrade;
import com.fxrisk.producer.FXTradeProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main application class to run the FX risk monitoring system.
 */
public class FXRiskApplication {
    private static final Logger logger = LoggerFactory.getLogger(FXRiskApplication.class);
    
    public static void main(String[] args) {
        logger.info("Starting FX Risk Monitoring Application");
        
        // Create thread pool using virtual threads (Java 21 feature)
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        
        // Start the consumers
        FXRiskAlertConsumer riskConsumer = new FXRiskAlertConsumer("fx-risk-group");
        AlertNotificationConsumer notificationConsumer = new AlertNotificationConsumer("alert-notification-group");
        
        executorService.submit(riskConsumer);
        executorService.submit(notificationConsumer);
        
        logger.info("Consumers started successfully");
        
        // Demo: Generate and publish sample trades
        if (args.length > 0 && "demo".equalsIgnoreCase(args[0])) {
            executorService.submit(() -> generateSampleTrades());
        }
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down application...");
            
            // Close the consumers
            try {
                riskConsumer.close();
                notificationConsumer.close();
            } catch (Exception e) {
                logger.error("Error closing consumers", e);
            }
            
            // Shutdown the executor service
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            logger.info("Application shutdown complete");
        }));
        
        // Keep the application running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Generates sample trades for demonstration purposes.
     */
    private static void generateSampleTrades() {
        logger.info("Generating sample FX trades for demonstration");
        
        try (FXTradeProducer producer = new FXTradeProducer()) {
            // Create sample trades
            List<FXTrade> sampleTrades = List.of(
                // Normal risk trades
                new FXTrade("EUR/USD", new BigDecimal("100000"), new BigDecimal("1.08123"), 
                            "BUY", "TRADER1", "BANK_A"),
                new FXTrade("GBP/USD", new BigDecimal("50000"), new BigDecimal("1.27543"), 
                            "SELL", "TRADER2", "BANK_B"),
                            
                // High risk trades
                new FXTrade("USD/MXN", new BigDecimal("15000000"), new BigDecimal("17.8901"), 
                            "BUY", "TRADER3", "HIGH_RISK_COUNTERPARTY"),
                new FXTrade("EUR/TRY", new BigDecimal("5000000"), new BigDecimal("31.5678"), 
                            "SELL", "TRADER4", "UNKNOWN")
            );
            
            // Publish trades with delays between them
            for (FXTrade trade : sampleTrades) {
                try {
                    producer.publishTradeSync(trade, Duration.ofSeconds(5));
                    // Sleep between trade publications
                    Thread.sleep(5000);
                } catch (Exception e) {
                    logger.error("Error publishing sample trade", e);
                }
            }
            
            logger.info("Sample trades generation completed");
            
        } catch (Exception e) {
            logger.error("Error in sample trade generation", e);
        }
    }
}
