package com.example.delivery.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class ChatResponseDTO {
    private String orderId;
    private String userId;
    private String role;
    private String message;
    private long timestamp;
}
