package com.example.delivery.service;

import com.example.delivery.dto.ChatMessageSqsRequestDTO;
import com.example.delivery.dto.ChatRequestDTO;
import com.example.delivery.dto.ChatResponseDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class ChatProducer {

    private final SqsClient sqsClient;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.cloud.aws.sqs.queue-url-chat}")
    private String queueUrl;

    // ✅ 메시지 전송 메서드
    public void sendMessage(ChatRequestDTO chatRequestDTO) {
        try {
            long timestamp = System.currentTimeMillis();
            log.info("🕒 Generated Timestamp: {}", timestamp);

            String deduplicationId = chatRequestDTO.getOrderId() + "_" + timestamp;

            // ✅ JSON 변환 (timestamp 포함)
            ChatMessageSqsRequestDTO chatMessage =
                    ChatMessageSqsRequestDTO.builder()
                            .Role(chatRequestDTO.getRole())
                            .orderId(chatRequestDTO.getOrderId())
                            .userId(chatRequestDTO.getUserId())
                            .message(chatRequestDTO.getMessage())
                            .timestamp(String.valueOf(timestamp)  )
                    .build();
            String messageJson = objectMapper.writeValueAsString(chatMessage);

            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(messageJson)
                    .messageGroupId(chatRequestDTO.getOrderId())
                    .messageDeduplicationId(deduplicationId)
                    .build();

            sqsClient.sendMessage(sendMessageRequest);
            log.info("✅ SQS 메시지 전송 완료: {}", messageJson);
        } catch (Exception e) {
            log.error("❌ SQS 메시지 전송 실패: {}", e.getMessage(), e);
        }
    }


    // ✅ 메시지 수신 및 Redis 저장
    @Async
    @SqsListener("${spring.cloud.aws.sqs.queue-name-chat}")
    public void processMessage(Message message) {
        try {
            log.info("📩 Received SQS message: {}", message.body());

            String messageBody = message.body();
            String receiptHandle = message.receiptHandle();

            // ✅ JSON 파싱
            ChatMessageSqsRequestDTO chatMessage = objectMapper.readValue(messageBody, ChatMessageSqsRequestDTO.class);

            if (chatMessage.getOrderId() == null || chatMessage.getUserId() == null) {
                log.error("🚨 Missing required fields in JSON message: {}", messageBody);
                return;
            }

            // ✅ timestamp 처리 (기본값: 현재 시간)
            long timestamp = parseTimestamp(chatMessage.getTimestamp());

            // ✅ Redis에 저장할 ChatResponseDTO 생성
            ChatResponseDTO chatResponseDTO = ChatResponseDTO.builder()
                    .timestamp(timestamp)
                    .message(chatMessage.getMessage())
                    .orderId(chatMessage.getOrderId())
                    .userId(chatMessage.getUserId())
                    .message(chatMessage.getMessage())
                    .role(chatMessage.getRole())
                    .build();
            String redisKey = "chat:" + chatMessage.getOrderId();
            redisTemplate.opsForList().rightPush(redisKey, objectMapper.writeValueAsString(chatResponseDTO));
            setRedisKeyExpiration(redisKey);

            // ✅ SQS 메시지 삭제
            deleteMessageFromSqs(receiptHandle);

            log.info("✅ Message successfully stored in Redis: {}", redisKey);
        } catch (JsonProcessingException jsonEx) {
            log.error("❌ JSON Parsing Error: {}", jsonEx.getMessage(), jsonEx);
        } catch (Exception e) {
            log.error("❌ Unexpected Error processing message: {}", e.getMessage(), e);
        }
    }

    // ✅ SQS 메시지 삭제 메서드
    private void deleteMessageFromSqs(String receiptHandle) {
        try {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build());
            log.info("🗑️ SQS 메시지 삭제 완료: {}", receiptHandle);
        } catch (Exception e) {
            log.error("❌ SQS 메시지 삭제 실패: {}", e.getMessage(), e);
        }
    }

    // ✅ timestamp 변환 메서드
    private long parseTimestamp(String timestampStr) {
        try {
            if (timestampStr == null || timestampStr.isEmpty()) {
                log.warn("⚠️ Missing timestamp, using current system time.");
                return System.currentTimeMillis();
            }
            return Long.parseLong(timestampStr);
        } catch (NumberFormatException e) {
            log.error("🚨 Invalid timestamp format: {}", timestampStr);
            return System.currentTimeMillis();
        }
    }

    // ✅ Redis에서 메시지 가져오기 (주문 기준, 특정 timestamp 이후 메시지만 가져오기)
    public List<Object> getMessagesFromRedis(String orderId, long fromTimestamp) {
        String redisKey = "chat:" + orderId;
        List<Object> allMessages = redisTemplate.opsForList().range(redisKey, 0, -1);

        if (allMessages == null || allMessages.isEmpty()) {
            return List.of();
        }

        return allMessages.stream()
                .map(message -> {
                    try {
                        ChatResponseDTO chatResponseDTO = objectMapper.readValue((String) message, ChatResponseDTO.class);
                        return chatResponseDTO;
                    } catch (Exception e) {
                        log.error("❌ JSON 파싱 실패: {}", message, e);
                        return null;
                    }
                })
                .filter(chatResponseDTO -> chatResponseDTO != null && chatResponseDTO.getTimestamp() > fromTimestamp)
                .sorted(Comparator.comparing(ChatResponseDTO::getTimestamp))  // ✅ 정렬 추가
                .map(chatResponseDTO -> (Object) chatResponseDTO)
                .toList();
    }



    // ✅ Redis 키 만료 시간 설정 (기본 1일)
    private void setRedisKeyExpiration(String key) {
        redisTemplate.expire(key, 1, TimeUnit.DAYS);
    }

    // ✅ Redis에서 특정 주문의 메시지 삭제
    public void deleteChatMessagesFromRedis(String orderId) {
        try {
            String redisKey = "chat:" + orderId;
            redisTemplate.delete(redisKey);
            log.info("🗑️ All chat messages for order {} have been deleted from Redis.", orderId);
        } catch (Exception e) {
            log.error("❌ Redis 메시지 삭제 실패: {}", e.getMessage(), e);
        }
    }
}
