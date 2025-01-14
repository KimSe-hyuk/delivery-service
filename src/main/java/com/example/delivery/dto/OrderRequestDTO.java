package com.example.delivery.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderRequestDTO {
    private String userId;
    private String message;
    private String status;
    private String orderId;
    private String riderId;
}
