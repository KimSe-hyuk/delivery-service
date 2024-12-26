package com.example.deliveryservice.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ChatRequestDTO {
    private String userId;
    private String productId;
    private String role;
    private String message;
}
