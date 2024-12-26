package com.example.deliveryservice.controller;

import com.example.deliveryservice.dto.ChatRequestDTO;
import com.example.deliveryservice.service.ChatProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/chat")
@RequiredArgsConstructor
public class ChatController {

    private final ChatProducer chatProducer;

    // 메시지 전송 API (고객 또는 라이더가 채팅 메시지 전송)
    @PostMapping("/send")
    @CrossOrigin(origins = "http://your-frontend-url.com")  // 외부 클라이언트의 URL을 지정
    public ResponseEntity<String> sendMessage(@RequestBody ChatRequestDTO chatRequestDTO) {
        try {
            // 메시지 보내기
            chatProducer.sendMessage(chatRequestDTO.getUserId(), chatRequestDTO.getProductId(),
                    chatRequestDTO.getRole(), chatRequestDTO.getMessage());
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            // 예외 발생 시 실패 응답 반환
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to send message: " + e.getMessage());
        }
    }

    // 메시지 수신 API (메시지를 큐에서 받아오기)
    @GetMapping("/receive")
    @CrossOrigin(origins = "http://your-frontend-url.com")  // 외부 클라이언트의 URL을 지정
    public ResponseEntity<List<String>> receiveMessages() {
        try {
            // 메시지 수신 및 처리
            List<String> messages = chatProducer.receiveMessages();
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            // 예외 발생 시 실패 응답 반환
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(List.of("Failed to receive messages: " + e.getMessage()));
        }
    }

    // 예외를 처리하는 글로벌 예외 처리기
    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleException(Exception e) {
        // 예외 발생 시 응답 반환
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Unexpected error occurred: " + e.getMessage());
    }
}
