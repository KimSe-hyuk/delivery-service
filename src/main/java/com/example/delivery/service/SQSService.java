package com.example.delivery.service;

import com.example.delivery.dto.OrderResponseDTO;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
@Service
@RequiredArgsConstructor
public class SQSService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final SqsClient sqsClient;

    @Value("${spring.cloud.aws.sqs.queue-url-deliveryStatus}")
    private String queueUrl;

    private static final String REDIS_ORDER_STATUSES_KEY = "orderStatuses";
    private static final String REDIS_ORDER_BODIES_KEY = "orderBodies";
    private static final String REDIS_ORDER_USER_IDS_KEY = "orderUsers";
    private static final String REDIS_ORDER_RIDER_IDS_KEY = "riderUsers";

    // 메시지 전송
    public void sendMessage(String userId, String message, String status, String orderId, String riderId) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Map<String, MessageAttributeValue> messageAttributes = buildMessageAttributes(status, timestamp, orderId, userId, riderId);

        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .messageAttributes(messageAttributes)
                .build();

        sqsClient.sendMessage(request);
        System.out.printf("Message %s sent for user %s with status %s at %s orderID %s riderId %s%n", message, userId, status, timestamp, orderId, riderId);
    }

    @SqsListener("${spring.cloud.aws.sqs.queue-name-deliveryStatus}")
    public void processMessage(@Payload Message message) {
        System.out.println("SQS 메시지: " + message);
        Map<String, MessageAttributeValue> attributes = message.messageAttributes();

        // 속성이 누락되었거나 null일 경우 디폴트 값 사용
        String orderId = getAttributeValue(attributes, "orderId", "defaultOrderId");
        String timestamp = getAttributeValue(attributes, "timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String status = getAttributeValue(attributes, "status", "defaultStatus");
        String userId = getAttributeValue(attributes, "userId", "defaultUserId");
        String riderId = getAttributeValue(attributes, "riderId", "defaultRiderId"); // riderId 추가
        String messageBody = message.body();

        System.out.printf("orderId: %s, timestamp: %s, status: %s, userId: %s, riderId: %s, messageBody: %s%n", orderId, timestamp, status, userId, riderId, messageBody);

        String currentStatus = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
        String currentRiderId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_RIDER_IDS_KEY, orderId);

        // 상태가 동일하고 riderId도 동일하면 처리하지 않고 종료
        if (status.equals(currentStatus) && riderId.equals(currentRiderId)) {
            System.out.printf("주문 ID %s는 동일한 상태와 riderId입니다. 업데이트를 건너뜁니다.%n", orderId);
            return;
        }

        // 상태가 변경된 경우 기존 상태 및 riderId 제거
        if (currentStatus != null) {
            String oldZsetKey = "orderTimestamps:" + currentStatus;
            redisTemplate.opsForZSet().remove(oldZsetKey, orderId);
            redisTemplate.opsForHash().delete(REDIS_ORDER_STATUSES_KEY, orderId);
            redisTemplate.opsForHash().delete(REDIS_ORDER_RIDER_IDS_KEY, orderId); // 기존 riderId 제거
            System.out.printf("주문 ID %s의 상태가 %s에서 %s로 업데이트되었습니다.%n", orderId, currentStatus, status);
        }

        // 새로운 상태로 추가
        double score = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .toEpochSecond(ZoneOffset.UTC);
        String newZsetKey = "orderTimestamps:" + status;
        redisTemplate.opsForZSet().add(newZsetKey, orderId, score);
        redisTemplate.opsForHash().put(REDIS_ORDER_STATUSES_KEY, orderId, status);
        redisTemplate.opsForHash().put(REDIS_ORDER_BODIES_KEY, orderId, messageBody);
        redisTemplate.opsForHash().put(REDIS_ORDER_USER_IDS_KEY, orderId, userId);

        // riderId도 상태에 따라 업데이트
        if ("배달중".equals(status) || "배달완료".equals(status)) {
            redisTemplate.opsForHash().put(REDIS_ORDER_RIDER_IDS_KEY, orderId, riderId);
        }

        deleteMessage(message.receiptHandle());
        System.out.printf("주문 ID %s의 상태가 %s로 업데이트되었습니다. (userId: %s, riderId: %s)%n", orderId, status, userId, riderId);
    }



    // 메시지 속성에서 안전하게 값을 가져오는 헬퍼 메소드 (디폴트 값 적용)
    private String getAttributeValue(Map<String, MessageAttributeValue> attributes, String attributeName, String defaultValue) {
        // 속성이 존재하지 않거나 null일 경우 디폴트 값 사용
        MessageAttributeValue value = attributes.get(attributeName);
        if (value == null || value.stringValue() == null) {
            System.out.printf("속성 %s가 누락되었거나 null입니다. 디폴트 값 '%s'를 사용합니다.%n", attributeName, defaultValue);
            return defaultValue;  // 디폴트 값 반환
        }
        return value.stringValue();
    }

    // Redis에서 특정 상태 및 orderId로 주문 가져오기
    public List<OrderResponseDTO> getOrderByStatusAndId(String statusFilter, String orderId) {
        // 상태별 ZSet 키 생성
        String zsetKey = "orderTimestamps:" + statusFilter;

        // ZSet에서 orderId 존재 여부 확인
        Double score = redisTemplate.opsForZSet().score(zsetKey, orderId);
        if (score == null) {
            return Collections.emptyList();  // 상태와 orderId가 일치하지 않으면 빈 리스트 반환
        }

        // Hash에서 status, messageBody, userId 가져오기
        String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
        String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, orderId);
        String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, orderId);

        if (status == null || messageBody == null || userId == null) {
            return Collections.emptyList();  // 상태, 메시지 본문, 사용자 ID 중 하나라도 없으면 빈 리스트 반환
        }

        // OrderResponseDTO로 변환
        OrderResponseDTO order = OrderResponseDTO.builder()
                .orderId(orderId)
                .status(status)
                .messageBody(messageBody)
                .userId(userId)
                .build();

        // 하나의 order만 반환하므로 List로 묶어서 반환
        return List.of(order);
    }


    // Redis에서 특정 orderId로 주문 가져오기
    public List<OrderResponseDTO> getOrdersByOrderId(String orderId) {
        List<OrderResponseDTO> orders = new ArrayList<>();

        // Hash에서 해당 orderId에 대한 상태, 메시지 본문, 사용자 ID 가져오기
        String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
        String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, orderId);
        String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, orderId);

        if (status == null || messageBody == null || userId == null) {
            return orders;  // 상태, 메시지 본문, 사용자 ID 중 하나라도 없으면 빈 리스트 반환
        }

        // toOrderResponseDTO 메서드를 사용하여 변환
        OrderResponseDTO order = toOrderResponseDTO(orderId, status, messageBody, userId);
        orders.add(order);  // 해당 orderId에 대한 주문을 리스트에 추가

        return orders;  // orderId별로 그룹화된 주문 정보 반환
    }




    // Redis에서 특정 상태인 주문만 가져오기
    public List<OrderResponseDTO> getDeliveriesByStatus(String statusFilter) {
        Set<Object> orderIds = redisTemplate.opsForZSet().reverseRange("orderTimestamps:" + statusFilter, 0, -1);
        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();  // 빈 리스트 반환
        }

        // 각 주문에 대해 상태, 주문 ID, 메시지 본문, 사용자 ID를 가져와 리스트로 반환
        return orderIds.stream().map(orderId -> {
            String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
            String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, orderId);
            String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, orderId);

            // 값이 하나라도 없으면 필터링
            if (status != null && messageBody != null && userId != null) {
                // OrderResponseDTO 객체로 변환하여 반환
                 return toOrderResponseDTO((String) orderId, status, messageBody, userId);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());  // null 값을 필터링하여 반환
    }

    public List<OrderResponseDTO> getDeliveriesByRiderId(String riderId) {
        // Redis에서 riderId에 해당하는 모든 주문 ID를 가져옴
        Map<Object, Object> ordersByRider = redisTemplate.opsForHash().entries(REDIS_ORDER_RIDER_IDS_KEY);

        // riderId에 매핑된 주문 ID들을 필터링
        Set<Object> orderIds = ordersByRider.entrySet().stream()
                .filter(entry -> riderId.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (orderIds.isEmpty()) {
            return Collections.emptyList();  // 빈 리스트 반환
        }

        // 각 주문에 대해 상태, 주문 ID, 메시지 본문, 사용자 ID를 가져와 리스트로 반환
        return orderIds.stream().map(orderId -> {
            String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
            String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, orderId);
            String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, orderId);

            // 값이 하나라도 없으면 필터링
            if (status != null && messageBody != null && userId != null) {
                // OrderResponseDTO 객체로 변환하여 반환
                return toOrderResponseDTO((String) orderId, status, messageBody, userId);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());  // null 값을 필터링하여 반환
    }

    // SQS 메시지 삭제
    private void deleteMessage(String receiptHandle) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();

        sqsClient.deleteMessage(request);
        System.out.printf("Deleted SQS message with receipt handle: %s%n", receiptHandle);
    }

    // 메시지 속성 빌드
    private Map<String, MessageAttributeValue> buildMessageAttributes(String status, String currentDateTime, String orderId, String userId,String riderId) {
        return Map.of(
                "status", MessageAttributeValue.builder().dataType("String").stringValue(status).build(),
                "timestamp", MessageAttributeValue.builder().dataType("String").stringValue(currentDateTime).build(),
                "orderId", MessageAttributeValue.builder().dataType("String").stringValue(orderId).build(),
                "userId", MessageAttributeValue.builder().dataType("String").stringValue(userId).build(),  // userId 추가
                "riderId", MessageAttributeValue.builder().dataType("String").stringValue(riderId).build()  // userId 추가
        );
    }
    public List<OrderResponseDTO> getAllOrders() {
        List<OrderResponseDTO> orders = new ArrayList<>();

        // 모든 orderId 가져오기
        Set<Object> orderIds = redisTemplate.opsForHash().keys(REDIS_ORDER_STATUSES_KEY);
        if (orderIds == null || orderIds.isEmpty()) {
            return orders; // 주문이 없으면 빈 리스트 반환
        }

        // 각 orderId에 대한 정보를 수집
        for (Object orderId : orderIds) {
            String id = orderId.toString();
            String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, id);
            String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, id);
            String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, id);

            // 값이 하나라도 없으면 건너뜁니다
            if (status == null || messageBody == null || userId == null) {
                continue;
            }

            // OrderResponseDTO로 변환
            OrderResponseDTO order = OrderResponseDTO.builder()
                    .orderId(id)
                    .status(status)
                    .messageBody(messageBody)
                    .userId(userId)
                    .build();

            orders.add(order);
        }

        return orders;  // OrderResponseDTO 리스트 반환
    }

    public OrderResponseDTO toOrderResponseDTO(String orderId, String status, String messageBody, String userId) {
        return OrderResponseDTO.builder()
                .orderId(orderId)
                .status(status)
                .messageBody(messageBody)
                .userId(userId)
                .build();
    }

}


