package com.example.delivery.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class LocationRequestDTO {
    private String deliveryPersonId; // 배달원 ID
    private double latitude;
    private double longitude;
}
