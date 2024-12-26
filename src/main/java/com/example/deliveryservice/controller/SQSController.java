package com.example.deliveryservice.controller;

import com.example.deliveryservice.dto.OrderRequestDTO;
import com.example.deliveryservice.service.SQSService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/delivery")
@RequiredArgsConstructor
public class SQSController {

    private final SQSService sqsService;

    // 메시지 전송
    @PostMapping("/send")
    public String sendMessage(@RequestBody OrderRequestDTO orderRequestDTO) {
        sqsService.sendMessage(orderRequestDTO.getUserId(), orderRequestDTO.getMessage());
        return "Message sent successfully for userId: " + orderRequestDTO.getUserId();
    }

    // 특정 userId의 메시지 받기
    @GetMapping("/receive")
    public List<String> receiveMessages(@RequestParam String userId) {
        List<Message> messages = sqsService.receiveMessages(userId);
        return messages.stream()
                .map(Message::body)  // 메시지 내용만 반환 (SDK v2 스타일)
                .collect(Collectors.toList());
    }
    @GetMapping("/receiveAll")
    public List<String> receiveMessages() {
        return sqsService.receiveAllMessages();
    }
    @DeleteMapping("/delete")
    public String deleteMessage(@RequestParam String userId) {
        sqsService.deleteMessagesByUserId(userId);
        return "Message deleted successfully for userId: " + userId;
    }
}
