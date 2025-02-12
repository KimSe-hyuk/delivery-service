package com.example.delivery.controller;

import com.example.delivery.dto.ChatRequestDTO;
import com.example.delivery.service.ChatProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/chat")
@RequiredArgsConstructor
@Slf4j
public class ChatController {

    private final ChatProducer chatProducer;

    // ✅ 메시지 전송 API (고객 또는 라이더가 채팅 메시지 전송)
    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestBody ChatRequestDTO chatRequestDTO) {
        try {
            // 메시지 보내기
            chatProducer.sendMessage(chatRequestDTO);
            log.info("✅ Message sent successfully: {}", chatRequestDTO);
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            // 예외 발생 시 실패 응답 반환
            log.error("❌ Failed to send message: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send message: " + e.getMessage());
        }
    }

    // ✅ 메시지 가져오기 API
    @GetMapping("/messages")
    public ResponseEntity<List<Object>> getMessages(
            @RequestParam String orderId,
            @RequestParam(required = false) Long fromTimestamp
    ) {
        try {
            log.info("📩 Fetching messages for orderId={}, fromTimestamp={}", orderId, fromTimestamp);

            // ✅ fromTimestamp가 null이면 기본값을 0으로 설정
            if (fromTimestamp == null) {
                fromTimestamp = 0L;
            }

            // Redis에서 메시지 가져오기
            List<Object> messagesFromRedis = chatProducer.getMessagesFromRedis(orderId, fromTimestamp);
            log.info("✅ Retrieved {} messages from Redis for orderId={}", messagesFromRedis.size(), orderId);

            return ResponseEntity.ok(messagesFromRedis);
        } catch (Exception e) {
            log.error("❌ Failed to fetch messages: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(List.of("Failed to fetch messages: " + e.getMessage()));
        }
    }
}
