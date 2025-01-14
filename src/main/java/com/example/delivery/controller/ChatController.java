package com.example.delivery.controller;

import com.example.delivery.dto.ChatMessageRequestDTO;
import com.example.delivery.dto.ChatRequestDTO;
import com.example.delivery.service.ChatProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/chat")
@RequiredArgsConstructor
public class ChatController {

    private final ChatProducer chatProducer;

    // 메시지 전송 API (고객 또는 라이더가 채팅 메시지 전송)
    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestBody ChatRequestDTO chatRequestDTO) {
        try {
            // 메시지 보내기
            chatProducer.sendMessage(chatRequestDTO);
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            // 예외 발생 시 실패 응답 반환
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send message: " + e.getMessage());
        }
    }


    @GetMapping("/messages")
    public ResponseEntity<List<Object>> getMessages(
            @RequestParam String orderId,
            @RequestParam Long fromTimestamp
 ) {
        try {
            // Redis Key 생성
            List<Object> messagesFromRedis = chatProducer.getMessagesFromRedis(orderId,fromTimestamp);
            System.out.println("messagesFromRedis: " + messagesFromRedis);
            return ResponseEntity.ok(messagesFromRedis);
        } catch (Exception e) {
            System.out.println("messagesFromRedisFail: " + e.getMessage());
            return ResponseEntity.status(500).body(List.of("Failed to fetch messages: " + e.getMessage()));
        }
    }

}
