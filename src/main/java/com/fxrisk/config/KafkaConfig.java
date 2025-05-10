package com.fxrisk.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Configuration class for Kafka clients used in the FX Risk monitoring system.
 * Provides standard configurations for consumers and producers with sensible defaults.
 */
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);
    private static final Properties envProperties = loadEnvProperties();

    // Topic names
    public static final String FX_TRADES_TOPIC = "fx-trades";
    public static final String FX_RISK_ALERTS_TOPIC = "fx-risk-alerts";
    
    // Default server configuration
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

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

    // Default topic configuration
    private static final int DEFAULT_PARTITIONS = 4;
    private static final short DEFAULT_REPLICATION_FACTOR = 1;
    
    /**
     * Creates properties for a Kafka consumer with the given consumer group ID.
     * Uses String key/value deserializers as all our messages are JSON strings.
     * 
     * @param groupId The consumer group ID
     * @return Properties configured for a Kafka consumer
     */
    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        
        // Connection settings
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        
        // Serialization settings
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Consumer behavior settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // Performance tuning
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        // Configure rebalancing behavior
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);

        // Add SASL/SSL configuration for Redpanda Serverless if needed
        if (USE_SASL) {
            configureSasl(props);
        }
        
        return props;
    }
    
    /**
     * Creates properties for a Kafka producer.
     * Uses String key/value serializers as all our messages are JSON strings.
     * 
     * @return Properties configured for a Kafka producer
     */
    public static Properties getProducerProperties() {
        Properties props = new Properties();
        
        // Connection settings
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        
        // Serialization settings
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG);
        props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG);
        
        // Performance tuning
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // Enable idempotent producer to avoid duplicates
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Add SASL/SSL configuration for Redpanda Serverless if needed
        if (USE_SASL) {
            configureSasl(props);
        }

        return props;
    }
    
    /**
     * Creates properties for a Kafka AdminClient.
     * 
     * @return Properties configured for a Kafka AdminClient
     */
    public static Properties getAdminProperties() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return props;
    }
    
    /**
     * Ensures that all required Kafka topics exist before application startup.
     * If topics don't exist, they will be created with default configuration.
     * If Kafka is not available, logs a warning but allows the application to continue.
     */
    public static void ensureTopicsExist() {
        logger.info("Ensuring required Kafka topics exist...");
        
        List<NewTopic> topics = Arrays.asList(
            new NewTopic(FX_TRADES_TOPIC, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR),
            new NewTopic(FX_RISK_ALERTS_TOPIC, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR)
        );
        
        Properties adminProps = getAdminProperties();
        // Add a reasonable connection timeout
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // First check if topics already exist
            Set<String> existingTopics;
            try {
                existingTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                logger.warn("Failed to connect to Kafka for topic verification. Is Kafka running at {}?", 
                    adminProps.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
                logger.warn("The application will continue but might fail when trying to produce or consume messages");
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return; // Continue without creating topics
            }
            
            // Filter out topics that already exist
            List<NewTopic> topicsToCreate = topics.stream()
                .filter(topic -> !existingTopics.contains(topic.name()))
                .collect(Collectors.toList());
                
            if (topicsToCreate.isEmpty()) {
                logger.info("All required Kafka topics already exist: {} and {}", 
                    FX_TRADES_TOPIC, FX_RISK_ALERTS_TOPIC);
                return;
            }
            
            // Create only topics that don't exist
            CreateTopicsResult createTopicsResult = adminClient.createTopics(topicsToCreate);
            
            // Wait for topic creation to complete
            for (NewTopic topic : topicsToCreate) {
                try {
                    createTopicsResult.values().get(topic.name()).get(5, TimeUnit.SECONDS);
                    logger.info("Successfully created Kafka topic: {}", topic.name());
                } catch (TimeoutException e) {
                    logger.warn("Timeout while creating topic {}", topic.name());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        logger.info("Topic {} already exists", topic.name());
                    } else {
                        logger.error("Error creating topic {}: {}", topic.name(), e.getMessage());
                    }
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while creating topic {}", topic.name());
                    Thread.currentThread().interrupt();
                }
            }
            
            logger.info("Kafka topic verification completed");
        } catch (Exception e) {
            logger.error("Failed to ensure Kafka topics exist", e);
            logger.warn("The application will continue but might fail when trying to produce or consume messages");
        }
    }
    
    /**
     * Gets the bootstrap servers configuration, checking system properties and 
     * environment variables before falling back to default.
     * 
     * @return Bootstrap servers connection string
     */
    private static String getBootstrapServers() {
        // Check system property
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers");
        
        // If not found, check environment variable
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            bootstrapServers = getProperty("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        }
        return bootstrapServers;
    }

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
