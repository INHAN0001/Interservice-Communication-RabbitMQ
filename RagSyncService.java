package com.balanceiq.core;

import com.balanceiq.config.RabbitMQConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class RagSyncService {

    private final RabbitTemplate rabbitTemplate;

    /**
     * Publish a RAG sync event scoped to a specific company.
     * The RAG service routes the content into that company's Qdrant collection.
     */
    public void syncToRag(String content, String feature, String companyName) {
        if (companyName == null || companyName.trim().isEmpty()) {
            throw new IllegalArgumentException("companyName must not be null or empty for RAG synchronization.");
        }
        publish(content, feature, companyName.trim());
    }

    /**
     * Calls the RAG service /reset endpoint to wipe a specific company's collection
     * before a full re-sync, preventing duplicate records.
     */
    private void publish(String content, String feature, String companyName) {
        try {
            Map<String, String> message = new HashMap<>();
            message.put("content", content);
            message.put("feature", feature);
            message.put("companyName", companyName);
            rabbitTemplate.convertAndSend(
                RabbitMQConfig.RAG_SYNC_EXCHANGE,
                RabbitMQConfig.RAG_SYNC_KEY,
                message
            );
            log.debug("Published RAG sync event: feature={}, company={}", feature, companyName);
        } catch (Exception e) {
            log.error("Failed to publish RAG sync event: {}", e.getMessage());
        }
    }
}
