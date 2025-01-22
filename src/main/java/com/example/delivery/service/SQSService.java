package com.example.delivery.service;

import com.example.delivery.dto.OrderResponseDTO;
import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class SQSService {

    private final ChatProducer chatProducer;
    private final RedisTemplate<String, Object> redisTemplate;
    private final SqsClient sqsClient;

    @Value("${spring.cloud.aws.sqs.queue-url-deliveryStatus}")
    private String queueUrl;
    private final RedisConnectionFactory connectionFactory;

    private static final String REDIS_ORDER_STATUSES_KEY = "orderStatuses";
    private static final String REDIS_ORDER_BODIES_KEY = "orderBodies";
    private static final String REDIS_ORDER_USER_IDS_KEY = "orderUsers";
    private static final String REDIS_ORDER_RIDER_IDS_KEY = "riderUsers";

    /**
     * SQS에 메시지를 전송하는 메서드입니다.
     * 주로 주문 상태 업데이트, 사용자 알림 등에 사용됩니다.
     */
    public void sendMessage(String userId, String message, String status, String orderId, String riderId) {
        String timestamp = getCurrentTimestamp();

        Map<String, MessageAttributeValue> messageAttributes = buildMessageAttributes(status, timestamp, orderId, userId, riderId);

        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .messageAttributes(messageAttributes)
                .build());

        System.out.printf("Message sent: userId=%s, status=%s, orderId=%s, riderId=%s, timestamp=%s%n", userId, status, orderId, riderId, timestamp);
    }

    /**
     * SQS로부터 메시지를 수신하여 처리하는 메서드입니다.
     * 메시지에 포함된 주문 정보를 기반으로 Redis에 데이터를 저장하거나 삭제합니다.
     */
    @SqsListener("${spring.cloud.aws.sqs.queue-name-deliveryStatus}")
    public void processMessage(@Payload Message message) {
        Map<String, MessageAttributeValue> attributes = message.messageAttributes();
        String orderId = getAttributeValue(attributes, "orderId", "defaultOrderId");
        String timestamp = getAttributeValue(attributes, "timestamp", getCurrentTimestamp());
        String status = getAttributeValue(attributes, "status", "defaultStatus");
        String userId = getAttributeValue(attributes, "userId", "defaultUserId");
        String riderId = getAttributeValue(attributes, "riderId", "defaultRiderId");

        System.out.printf("Processing SQS message: orderId=%s, status=%s, userId=%s, riderId=%s%n", orderId, status, userId, riderId);

        if ("배달끝".equals(status)) {
            chatProducer.deleteChatMessagesFromRedis(orderId);
            deleteOrderData(orderId);
            deleteMessage(message.receiptHandle());
            return;
        }

        updateOrderData(orderId, status, userId, riderId, timestamp, message.body());
        deleteMessage(message.receiptHandle());
    }

    /**
     * Redis에 주문 데이터를 업데이트하는 메서드입니다.
     * 기존 상태가 변경된 경우 데이터를 갱신하며, 새로운 상태로 추가합니다.
     */
    private void updateOrderData(String orderId, String status, String userId, String riderId, String timestamp, String messageBody) {
        String currentStatus = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
        String currentRiderId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_RIDER_IDS_KEY, orderId);

        if (status.equals(currentStatus) && riderId.equals(currentRiderId)) {
            System.out.printf("Order %s already up-to-date. Skipping.%n", orderId);
            return;
        }

        if (currentStatus != null) {
            redisTemplate.opsForZSet().remove("orderTimestamps:" + currentStatus, orderId);
            redisTemplate.opsForHash().delete(REDIS_ORDER_STATUSES_KEY, orderId);
            redisTemplate.opsForHash().delete(REDIS_ORDER_RIDER_IDS_KEY, orderId);
        }

        double time = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC);
        redisTemplate.opsForZSet().add("orderTimestamps:" + status, orderId, time);

        redisTemplate.opsForHash().put(REDIS_ORDER_STATUSES_KEY, orderId, status);
        redisTemplate.opsForHash().put(REDIS_ORDER_BODIES_KEY, orderId, messageBody);
        redisTemplate.opsForHash().put(REDIS_ORDER_USER_IDS_KEY, orderId, userId);

        if ("배달중".equals(status) || "배달완료".equals(status)) {
            redisTemplate.opsForHash().put(REDIS_ORDER_RIDER_IDS_KEY, orderId, riderId);
        }

        setRedisKeyExpirationForOrderId(REDIS_ORDER_STATUSES_KEY, orderId);  // 상태는 1일
        setRedisKeyExpirationForOrderId(REDIS_ORDER_BODIES_KEY, orderId);    // 메시지 본문은 7일
        setRedisKeyExpirationForOrderId(REDIS_ORDER_USER_IDS_KEY, orderId);  // 사용자 ID는 7일
        setRedisKeyExpirationForOrderId(REDIS_ORDER_RIDER_IDS_KEY, orderId); // 라이더 ID는 7일


        System.out.printf("Order %s updated in Redis with status=%s and riderId=%s%n", orderId, status, riderId);

    }

    /**
     * Redis에서 특정 주문 데이터를 삭제하는 메서드입니다.
     * 주로 "배달끝" 상태인 경우 호출됩니다.
     */
    private void deleteOrderData(String orderId) {
        redisTemplate.opsForHash().delete(REDIS_ORDER_STATUSES_KEY, orderId);
        redisTemplate.opsForHash().delete(REDIS_ORDER_BODIES_KEY, orderId);
        redisTemplate.opsForHash().delete(REDIS_ORDER_USER_IDS_KEY, orderId);
        redisTemplate.opsForHash().delete(REDIS_ORDER_RIDER_IDS_KEY, orderId);

        redisTemplate.opsForZSet().remove("orderTimestamps:배달끝", orderId);

        System.out.printf("Deleted Redis data for orderId=%s%n", orderId);
    }

    /**
     * SQS 메시지를 삭제하는 메서드입니다.
     * 메시지 처리가 완료된 후 호출됩니다.
     */
    private void deleteMessage(String receiptHandle) {
        sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build());
        System.out.printf("Deleted SQS message: receiptHandle=%s%n", receiptHandle);
    }

    /**
     * 메시지 속성을 생성하는 메서드입니다.
     * SQS에 전송될 메시지의 메타정보를 포함합니다.
     */
    private Map<String, MessageAttributeValue> buildMessageAttributes(String status, String timestamp, String orderId, String userId, String riderId) {
        return Map.of(
                "status", MessageAttributeValue.builder().dataType("String").stringValue(status).build(),
                "timestamp", MessageAttributeValue.builder().dataType("String").stringValue(timestamp).build(),
                "orderId", MessageAttributeValue.builder().dataType("String").stringValue(orderId).build(),
                "userId", MessageAttributeValue.builder().dataType("String").stringValue(userId).build(),
                "riderId", MessageAttributeValue.builder().dataType("String").stringValue(riderId).build()
        );
    }

    /**
     * 메시지 속성에서 값을 안전하게 가져오는 메서드입니다.
     * 속성이 존재하지 않을 경우 디폴트 값을 반환합니다.
     */
    private String getAttributeValue(Map<String, MessageAttributeValue> attributes, String key, String defaultValue) {
        MessageAttributeValue value = attributes.get(key);
        return value != null && value.stringValue() != null ? value.stringValue() : defaultValue;
    }

    // Redis 키의 만료 시간을 설정하는 메서드 (orderId 별로)
    private void setRedisKeyExpirationForOrderId(String key, String orderId) {
        String redisKey = key + ":" + orderId;  // orderId를 포함한 키
        redisTemplate.expire(redisKey, 1, TimeUnit.DAYS);  // orderId별로 만료 시간 설정
    }

    /**
     * 현재 시간을 yyyy-MM-dd HH:mm:ss 형식의 문자열로 반환하는 메서드입니다.
     */
    private String getCurrentTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Redis에서 특정 상태와 주문 데이터를 오래된 순서대로 조회하는 메서드입니다.
     */
    public List<OrderResponseDTO> getOrdersByStatusAndId(String statusFilter, String orderId) {
        // ZSet에서 상태에 해당하는 모든 데이터를 오래된 순서대로 가져오기
        Set<Object> orderIds = redisTemplate.opsForZSet().range("orderTimestamps:" + statusFilter, 0, -1);
        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();
        }

        // 오래된 순서로 반환된 ID 중에서 특정 orderId를 확인
        return orderIds.stream()
                .filter(id -> id.equals(orderId))
                .map(id -> buildOrderResponse((String) id))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    /**
     * 특정 주문 ID를 포함한 오래된 순서대로 주문 데이터를 조회하는 메서드입니다.
     */
    public List<OrderResponseDTO> getOrdersByOrderId(String orderId) {
        // 모든 상태의 ZSet에서 오래된 순서로 정렬된 데이터 가져오기
        Set<Object> orderIds = redisTemplate.opsForZSet().range("orderTimestamps:all", 0, -1);

        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();
        }

        // 반환된 ID 중에서 특정 orderId만 필터링
        return orderIds.stream()
                .filter(id -> id.equals(orderId))
                .map(id -> buildOrderResponse((String) id))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Redis 키를 기반으로 데이터를 오래된 순서대로 조회하는 공통 메서드.
     */
    private List<OrderResponseDTO> getOrders(String idFilter, String status, String keyType) {
        String redisKey = "orderTimestamps:" + status; // ZSet 키 결정

        // ZSet에서 오래된 순서대로 데이터 조회
        Set<Object> orderIds = redisTemplate.opsForZSet().range(redisKey, 0, -1);

        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();
        }

        // 오래된 순서로 정렬된 데이터에서 ID 필터링 및 응답 빌드
        return orderIds.stream()
                .map(orderId -> buildOrderResponse((String) orderId)) // 주문 응답 빌드
                .filter(order -> order != null && idFilter.equals(getFilterKey(order, keyType))) // ID 및 상태 필터링
                .collect(Collectors.toList());
    }

    /**
     * Redis 키를 기반으로 데이터를 다중 상태와 오래된 순서대로 조회하는 공통 메서드.
     */
    private List<OrderResponseDTO> getOrdersByMultipleStatuses(String idFilter, List<String> statuses, String keyType) {
        return statuses.stream()
                .flatMap(status -> getOrders(idFilter, status, keyType).stream()) // 상태별로 데이터 조회 후 합침
                .sorted(Comparator.comparing(OrderResponseDTO::getOrderId)) // 정렬
                .collect(Collectors.toList());
    }

    /**
     * 사용자 ID와 배달중, 배달완료로 주문 데이터를 조회.
     */
    public List<OrderResponseDTO> getUserIdList(String userId) {
        List<String> deliveryStatuses = Arrays.asList("배달중", "배달완료");
        return getOrdersByMultipleStatuses(userId, deliveryStatuses, "USER");
    }

    /**
     * 라이더 ID와 배달중, 배달완료로 주문 데이터를 조회.
     */
    public List<OrderResponseDTO> getRiderIdList(String riderId) {
        List<String> deliveryStatuses = Arrays.asList("배달중", "배달완료");
        return getOrdersByMultipleStatuses(riderId, deliveryStatuses, "RIDER");
    }

    /**
     * 사용자 ID와 특정 상태로 주문 데이터를 조회.
     */
    public List<OrderResponseDTO> getOrdersByUserId(String userId, String status) {
        return getOrders(userId, status, "USER");
    }

    /**
     * 라이더 ID와 특정 상태로 주문 데이터를 조회.
     */
    public List<OrderResponseDTO> getDeliveriesByRiderId(String riderId, String status) {
        return getOrders(riderId, status, "RIDER");
    }

    /**
     * 필터링에 사용할 키 결정 메서드.
     */
    private String getFilterKey(OrderResponseDTO order, String keyType) {
        return switch (keyType) {
            case "USER" -> order.getUserId();
            case "RIDER" -> order.getRiderId();
            default -> throw new IllegalArgumentException("Invalid keyType: " + keyType);
        };
    }



    /**
     * 특정 상태의 주문 데이터를 조회하는 메서드입니다.
     */
    public List<OrderResponseDTO> getDeliveriesByStatus(String statusFilter) {
        Set<Object> orderIds = redisTemplate.opsForZSet().reverseRange("orderTimestamps:" + statusFilter, 0, -1);
        return orderIds == null ? Collections.emptyList() : orderIds.stream()
                .map(orderId -> buildOrderResponse((String) orderId))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }


    /**
     * 주문 ID를 기반으로 OrderResponseDTO 객체를 생성하는 메서드입니다.
     */
    private OrderResponseDTO buildOrderResponse(String orderId) {
        String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, orderId);
        String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, orderId);
        String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, orderId);
        String riderId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_RIDER_IDS_KEY, orderId);

        if (status == null || messageBody == null || userId == null) {
            return null;
        }

        return OrderResponseDTO.builder()
                .orderId(orderId)
                .status(status)
                .messageBody(messageBody)
                .userId(userId)
                .riderId(riderId)
                .build();
    }
    // 모든 주문 내역 가져오기
    public List<OrderResponseDTO> getAllOrders() {
        List<OrderResponseDTO> orders = new ArrayList<>();

        // 모든 orderId 가져오기
        Set<Object> orderIds = redisTemplate.opsForHash().keys(REDIS_ORDER_STATUSES_KEY);
        if (orderIds.isEmpty()) {
            return orders; // 주문이 없으면 빈 리스트 반환
        }

        // 각 orderId에 대한 정보를 수집
        for (Object orderId : orderIds) {
            String id = orderId.toString();
            String status = (String) redisTemplate.opsForHash().get(REDIS_ORDER_STATUSES_KEY, id);
            String messageBody = (String) redisTemplate.opsForHash().get(REDIS_ORDER_BODIES_KEY, id);
            String userId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_USER_IDS_KEY, id);
            String riderId = (String) redisTemplate.opsForHash().get(REDIS_ORDER_RIDER_IDS_KEY, id);

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
                    .riderId(riderId) // riderId 추가
                    .build();

            orders.add(order);
        }

        return orders;  // OrderResponseDTO 리스트 반환
    }


    public void deleteRedis() {
        connectionFactory.getConnection().serverCommands().flushDb();
    }
}
