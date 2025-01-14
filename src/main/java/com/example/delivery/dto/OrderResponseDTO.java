package com.example.delivery.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class OrderResponseDTO {
    private String status;
    private String orderId;
    private String messageBody;
    private String userId;
    private String riderId;

}
