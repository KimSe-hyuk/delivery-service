package com.example.delivery.controller;

import com.example.delivery.dto.LocationRequestDTO;
import com.example.delivery.service.LocationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/location")
@RequiredArgsConstructor
public class LocationController {

    private final LocationService locationService;

        @PostMapping("/update-location")
    public ResponseEntity<String> updateLocation(@RequestBody LocationRequestDTO locationRequestDTO) {
        System.out.println(locationRequestDTO);
        try {
            // Service 호출
            return ResponseEntity.ok(locationService.updateLocation(locationRequestDTO));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Invalid location data: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("An unexpected error occurred: " + e.getMessage());
        }
    }
    @GetMapping("/get-delivery-location")
    public ResponseEntity<Map<String, Double>> getDeliveryLocation(@RequestParam String deliveryPersonId) {
        if (deliveryPersonId == null || deliveryPersonId.isEmpty()) {
            return ResponseEntity.badRequest().body(null);
        }
        return locationService.getDeliveryLocation(deliveryPersonId);
    }
}
