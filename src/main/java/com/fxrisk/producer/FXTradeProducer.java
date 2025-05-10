package com.fxrisk.producer;

import com.fxrisk.config.KafkaConfig;
import com.fxrisk.model.FXTrade;
import com.fxrisk.util.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;

public class FXTradeProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(FXTradeProducer.class);
    private final Producer<String, String> producer;
    private final ExecutorService callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public FXTradeProducer() {
        this.producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());
    }

    public CompletableFuture<RecordMetadata> publishTrade(FXTrade trade) {
        String key = trade.getTradeId();
        String value = JsonSerializer.serialize(trade);

        logger.info("Publishing FX trade: {} {} for {} at rate {}",
                trade.getCurrencyPair(),
                trade.getDirection(),
                trade.getAmount(),
                trade.getRate());

        CompletableFuture<RecordMetadata> resultFuture = new CompletableFuture<>();

        try {
            producer.send(
                new ProducerRecord<>(
                    KafkaConfig.FX_TRADES_TOPIC,
                    key, value
                ),
                (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Error sending FX trade: {}", key, exception);
                        resultFuture.completeExceptionally(exception);
                    } else {
                        logger.debug("FX trade sent to partition {} with offset {}",
                                metadata.partition(), metadata.offset());
                        resultFuture.complete(metadata);
                    }
                }
            );
            return resultFuture;
        } catch (Exception e) {
            logger.error("Failed to publish FX trade: {}", key, e);
            resultFuture.completeExceptionally(e);
            return resultFuture;
        }
    }

    public RecordMetadata publishTradeSync(FXTrade trade, Duration timeout)
            throws TimeoutException, ExecutionException, InterruptedException {
        CompletableFuture<RecordMetadata> future = publishTrade(trade);
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logger.error("Timed out waiting for trade publication confirmation: {}", trade.getTradeId());
            throw new TimeoutException("Publish operation timed out after " + timeout);
        }
    }

    @Override
    public void close() {
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
            logger.info("FX trade producer closed");
            callbackExecutor.shutdown();
            if (!callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Callback executor did not terminate in time, forcing shutdown");
                callbackExecutor.shutdownNow();
            }
        } catch (Exception e) {
            logger.error("Error closing producer", e);
        }
    }
}
