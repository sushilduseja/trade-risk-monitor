package com.fxrisk.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Configuration for Kafka/Redpanda producers and consumers.
 */
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    private static final Properties envProperties = loadEnvProperties();
    
    // Topic names
    public static final String FX_TRADES_TOPIC = "fx-trades";
    public static final String FX_RISK_ALERTS_TOPIC = "fx-risk-alerts";
    
    // Kafka/Redpanda configuration
    public static final String BOOTSTRAP_SERVERS = getProperty("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    // SASL configuration for Redpanda Serverless or other secure Kafka clusters
    public static final boolean USE_SASL = Boolean.parseBoolean(getProperty("KAFKA_USE_SASL", "false"));
    public static final String SASL_USERNAME = getProperty("KAFKA_SASL_USERNAME", null);
    public static final String SASL_PASSWORD = getProperty("KAFKA_SASL_PASSWORD", null);
    
    // Consumer configuration
    public static final String MAX_POLL_INTERVAL_MS = getProperty("KAFKA_MAX_POLL_INTERVAL_MS", "300000");
    public static final String SESSION_TIMEOUT_MS = getProperty("KAFKA_SESSION_TIMEOUT_MS", "10000");
    
    // Producer configuration
    public static final String ACKS_CONFIG = getProperty("KAFKA_ACKS_CONFIG", "all");
    public static final String RETRIES_CONFIG = getProperty("KAFKA_RETRIES_CONFIG", "3");
    public static final String LINGER_MS_CONFIG = getProperty("KAFKA_LINGER_MS_CONFIG", "1");
    
    /**
     * Load properties from .env file if it exists
     */
    private static Properties loadEnvProperties() {
        Properties properties = new Properties();
        Path envPath = Paths.get(".env");
        
        if (Files.exists(envPath)) {
            try (FileInputStream fis = new FileInputStream(envPath.toFile())) {
                properties.load(fis);
                logger.info("Loaded configuration from .env file");
            } catch (IOException e) {
                logger.warn("Failed to load .env file: {}", e.getMessage());
            }
        } else {
            logger.info(".env file not found, using environment variables only");
        }
        
        return properties;
    }
    
    /**
     * Get property from environment variables or .env file with fallback to default value
     */
    private static String getProperty(String key, String defaultValue) {
        // First check system environment variables
        String value = System.getenv(key);
        
        // If not found, check .env file
        if (value == null || value.isEmpty()) {
            value = envProperties.getProperty(key);
        }
        
        // If still not found, use default value
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
    
    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG);
        props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG);
        
        // Enable idempotent producer to avoid duplicates
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        
        // Add SASL/SSL configuration for Redpanda Serverless if needed
        if (USE_SASL) {
            configureSasl(props);
        }
        
        return props;
    }
    
    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // Configure rebalancing behavior
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        
        // Add SASL/SSL configuration for Redpanda Serverless if needed
        if (USE_SASL) {
            configureSasl(props);
        }
        
        return props;
    }
    
    private static void configureSasl(Properties props) {
        logger.info("Configuring SASL authentication for Kafka/Redpanda");
        
        if (SASL_USERNAME == null || SASL_PASSWORD == null) {
            logger.error("SASL authentication enabled but username or password is not set");
            throw new IllegalStateException("SASL credentials not configured. Set KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD.");
        }
        
        // Set security protocol
        props.put("security.protocol", "SASL_SSL");
        
        // Set SASL mechanism (SCRAM-SHA-256 is common for Redpanda)
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        
        // Set JAAS configuration
        String jaasConfig = String.format(
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
            SASL_USERNAME, SASL_PASSWORD
        );
        props.put("sasl.jaas.config", jaasConfig);
        
        // Trust all certificates (for development only, use proper trust store in production)
        props.put("ssl.endpoint.identification.algorithm", "");
        
        logger.info("SASL configuration completed");
    }
}