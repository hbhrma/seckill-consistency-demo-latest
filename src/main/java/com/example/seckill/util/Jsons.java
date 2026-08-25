package com.example.seckill.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class Jsons {

    private final ObjectMapper objectMapper;

    public Jsons(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("serialize message failed", e);
        }
    }

    public <T> T fromJson(byte[] bytes, Class<T> type) {
        try {
            return objectMapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("deserialize message failed", e);
        }
    }

    public <T> T fromJson(String text, Class<T> type) {
        try {
            return objectMapper.readValue(text, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("deserialize message failed", e);
        }
    }
}
