package com.fxrisk.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Utility class for serializing and deserializing objects to/from JSON.
 * Uses Jackson Object Mapper with consistent configuration.
 * 
 * @param <T> Type of object to serialize/deserialize
 */
public class JsonSerializer<T> {
    
    private final ObjectMapper objectMapper;
    
    /**
     * Creates a new JsonSerializer with default configuration.
     * - Indented output for better readability
     * - Java 8 date/time support
     * - Ignores unknown properties during deserialization
     * - Doesn't fail on empty beans
     */
    public JsonSerializer() {
        this.objectMapper = new ObjectMapper();
        
        // Configure mapper for consistent behavior
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    /**
     * Serializes an object to a JSON string.
     * 
     * @param object The object to serialize
     * @return JSON string representation of the object
     * @throws RuntimeException If serialization fails
     */
    public String serialize(T object) {
        if (object == null) {
            throw new IllegalArgumentException("Cannot serialize null object");
        }
        
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }
    
    /**
     * Deserializes a JSON string to an object of type T.
     * 
     * @param json The JSON string to deserialize
     * @param clazz The class of the target object
     * @return Deserialized object of type T
     * @throws RuntimeException If deserialization fails
     */
    public T deserialize(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            throw new IllegalArgumentException("Cannot deserialize null or empty JSON string");
        }
        
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON to object", e);
        }
    }
    
    /**
     * Provides access to the underlying ObjectMapper for custom configuration.
     * 
     * @return The ObjectMapper instance
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
