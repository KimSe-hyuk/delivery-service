package com.example.delivery.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class ChatMessageSqsRequestDTO {
    private String orderId;
    private String userId;
    private String message;
    private String Role;
    private String timestamp;
}
