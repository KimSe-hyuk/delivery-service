package com.example.delivery.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ChatMessageRequestDTO {
    private String orderId;
    private  Long fromTimestamp;
}
