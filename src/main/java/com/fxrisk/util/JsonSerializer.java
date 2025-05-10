package com.fxrisk.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for serializing and deserializing objects to/from JSON.
 */
public class JsonSerializer {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper objectMapper = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }

    /**
     * Serialize an object to JSON.
     *
     * @param object The object to serialize
     * @return JSON string representation of the object
     * @throws RuntimeException if serialization fails
     */
    public static String serialize(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize object: {}", object, e);
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    /**
     * Deserialize JSON to an object.
     *
     * @param json JSON string to deserialize
     * @param clazz Class to deserialize to
     * @param <T> Type of the object
     * @return Deserialized object
     * @throws RuntimeException if deserialization fails
     */
    public static <T> T deserialize(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            logger.error("Failed to deserialize JSON to {}: {}", clazz.getSimpleName(), json, e);
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }
}
