package com.example.delivery.service;

import com.example.delivery.dto.ChatMessageRequestDTO;
import com.example.delivery.dto.ChatRequestDTO;
import com.example.delivery.dto.ChatResponseDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ChatProducer {

    private final SqsClient sqsClient;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.cloud.aws.sqs.queue-url-chat}")
    private String queueUrl;


    // 메시지 전송 메서드
    public void sendMessage(ChatRequestDTO chatRequestDTO) {
        long timestamp = System.currentTimeMillis();
        System.out.println("Timestamp: " + timestamp);
        // 중복 방지를 위한 MessageDeduplicationId 생성
        String deduplicationId = chatRequestDTO.getOrderId() + "_" + timestamp;  // 예시: orderId와 timestamp 결합하여 고유 ID 생성

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(chatRequestDTO.getMessage())
                .messageGroupId(chatRequestDTO.getOrderId())  // `orderId`로 그룹화하여 순서 보장
                .messageDeduplicationId(deduplicationId)  // 중복 방지를 위한 고유 ID
                .messageAttributes(Map.of(
                        "orderId", MessageAttributeValue.builder().dataType("String").stringValue(chatRequestDTO.getOrderId()).build(),
                        "userId", MessageAttributeValue.builder().dataType("String").stringValue(chatRequestDTO.getUserId()).build(),
                        "role", MessageAttributeValue.builder().dataType("String").stringValue(chatRequestDTO.getRole()).build(),
                        "timestamp", MessageAttributeValue.builder().dataType("String").stringValue(String.valueOf(timestamp)).build()))
                .build();

        // 메시지 전송
        sqsClient.sendMessage(sendMessageRequest);
    }

    // 메시지 받기 및 Redis 저장 메서드
    @SqsListener("${spring.cloud.aws.sqs.queue-name-chat}")
    public void processMessage(Message message) {
        try {
            String receiptHandle = message.receiptHandle();
            String messageBody = message.body();
            String orderId = message.messageAttributes().get("orderId").stringValue();
            String userId = message.messageAttributes().get("userId").stringValue();
            String role = message.messageAttributes().get("role").stringValue();
            String timestampStr = message.messageAttributes().get("timestamp").stringValue();
            long timestamp = Long.parseLong(timestampStr);
            System.out.println("time"+timestamp);
            // Redis 키 생성
            String redisKey = "chat:" + orderId;
            setRedisKeyExpiration(redisKey);
            // Redis에 저장 (타임스탬프와 함께 메시지 저장)
            ChatResponseDTO chatResponseDTO = ChatResponseDTO.builder()
                    .orderId(orderId)
                    .role(role)
                    .userId(userId)
                    .message(messageBody)
                    .timestamp(timestamp) // 타임스탬프 추가
                    .build();
            redisTemplate.opsForList().rightPush(redisKey, objectMapper.writeValueAsString(chatResponseDTO));

            // SQS 메시지 삭제
            deleteMessageFromSqs(receiptHandle);

            // 로그
            System.out.println("Processed message for Order ID: " + orderId);
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    // SQS 메시지 삭제 메서드
    private void deleteMessageFromSqs(String receiptHandle) {
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build());
    }

    // Redis에서 메시지 가져오기 메서드 (주문 기준으로, 타임스탬프 이후의 메시지만 가져오기)
    public List<Object> getMessagesFromRedis(String orderId,long fromTimestamp) {
        String redisKey = "chat:" + orderId;
        List<Object> allMessages = redisTemplate.opsForList().range(redisKey, 0, -1);  // Redis 리스트의 모든 메시지 가져오기

        // 타임스탬프 기준으로 필터링
        if (allMessages == null || allMessages.isEmpty()) {
            return List.of();
        }

        return allMessages.stream()
                .map(message -> {
                    try {
                        return objectMapper.readValue((String) message, ChatResponseDTO.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(chatRequestDTO -> chatRequestDTO != null && chatRequestDTO.getTimestamp() >  fromTimestamp)
                .map(chatRequestDTO -> (Object) chatRequestDTO) // 반환 형식 맞추기
                .toList();
    }
    /**
     * Redis 키의 만료 시간을 설정하는 메서드입니다.
     * 기본적으로 1일로 설정됩니다.
     */
    private void setRedisKeyExpiration(String key) {
        redisTemplate.expire(key, 1, TimeUnit.DAYS);
    }
    public void deleteChatMessagesFromRedis(String orderId) {
        try {
            // Redis에서 해당 orderId에 대한 채팅 메시지 삭제
            String redisKey = "chat:" + orderId;

            // 해당 채팅 메시지가 저장된 리스트 삭제
            redisTemplate.delete(redisKey);

            // 만약 Redis 키의 만료 시간도 설정한 경우, 만료 시간도 삭제되므로 별도의 만료 설정을 지울 필요는 없습니다.
            // 만약 해당 키에 만료 시간 외에 추가적인 처리가 필요하다면, 해당 코드에서 확인할 수 있습니다.

            System.out.printf("All chat messages for order %s have been deleted from Redis.%n", orderId);
        } catch (Exception e) {
            System.err.println("Error deleting chat messages for order " + orderId + ": " + e.getMessage());
        }
    }

}
