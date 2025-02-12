package com.example.delivery.service;

import com.example.delivery.dto.LocationRequestDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class LocationService {
    private final RedisTemplate<String, String> redisTemplate;
    public String updateLocation(LocationRequestDTO locationRequestDTO) {
        validateLocationRequest(locationRequestDTO);
        // Redis 키 생성 및 데이터 설정
        String redisKey = "delivery:location:" + locationRequestDTO.getDeliveryPersonId();
        String locationData = locationRequestDTO.getLatitude() + "," + locationRequestDTO.getLongitude();
        // 추가 비즈니스 로직 (예: 외부 API 호출 등)

        // Redis에 데이터 저장 (1일 TTL 설정)
        redisTemplate.opsForValue().set(redisKey, locationData, Duration.ofDays(1));
        System.out.println("location save");
        return "Location updated successfully";
    }

    private void validateLocationRequest(LocationRequestDTO locationRequestDTO) {
        if (locationRequestDTO == null) {
            throw new IllegalArgumentException("Request body cannot be null");
        }

        if (locationRequestDTO.getDeliveryPersonId() == null || locationRequestDTO.getDeliveryPersonId().isEmpty()) {
            throw new IllegalArgumentException("Delivery person ID is required");
        }

        if (locationRequestDTO.getLatitude() == 0.0) {
            throw new IllegalArgumentException("Latitude cannot be 0.0");
        }

        if (locationRequestDTO.getLongitude() == 0.0) {
            throw new IllegalArgumentException("Longitude cannot be 0.0");
        }
    }

    public ResponseEntity<Map<String, Double>> getDeliveryLocation(String deliveryPersonId) {
        String redisKey = "delivery:location:" + deliveryPersonId;
        String locationData = redisTemplate.opsForValue().get(redisKey);

        if (locationData == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }

        try {
            String[] latLng = locationData.split(",");
            if (latLng.length != 2) {
                throw new IllegalArgumentException("Invalid location data format");
            }

            double latitude = Double.parseDouble(latLng[0]);
            double longitude = Double.parseDouble(latLng[1]);

            Map<String, Double> location = new HashMap<>();
            location.put("latitude", latitude);
            location.put("longitude", longitude);
            System.out.println("location"+ location.get("latitude") + "," + location.get("longitude"));
            return ResponseEntity.ok(location);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(null);
        }
    }

    public ResponseEntity<Map<String, Map<String, Double>>> getAllDeliveryLocations() {
        Set<String> keys = redisTemplate.keys("delivery:location:*");
        if (keys == null || keys.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }

        Map<String, Map<String, Double>> allLocations = new HashMap<>();

        for (String key : keys) {
            String locationData = redisTemplate.opsForValue().get(key);
            if (locationData != null) {
                try {
                    String[] latLng = locationData.split(",");
                    if (latLng.length == 2) {
                        double latitude = Double.parseDouble(latLng[0]);
                        double longitude = Double.parseDouble(latLng[1]);

                        Map<String, Double> location = new HashMap<>();
                        location.put("latitude", latitude);
                        location.put("longitude", longitude);

                        String deliveryPersonId = key.replace("delivery:location:", "");
                        allLocations.put(deliveryPersonId, location);
                    }
                } catch (Exception e) {
                    // Handle errors if needed
                }
            }
        }

        return ResponseEntity.ok(allLocations);
    }

}
