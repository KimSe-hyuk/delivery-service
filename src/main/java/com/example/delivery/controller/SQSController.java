package com.example.delivery.controller;

import com.example.delivery.dto.OrderRequestDTO;
import com.example.delivery.dto.OrderResponseDTO;
import com.example.delivery.service.SQSService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/order")
@RequiredArgsConstructor
public class SQSController {

    private final SQSService sqsService;

    // 메시지 전송
    @PostMapping("/send")
    public String sendMessage(@RequestBody OrderRequestDTO orderRequestDTO) {
        String riderId = orderRequestDTO.getRiderId();
        if (riderId == null || riderId.isEmpty()) {
            riderId = "defaultRiderId";  // 기본값 설정
        }
        sqsService.sendMessage(orderRequestDTO.getUserId(), orderRequestDTO.getMessage(),orderRequestDTO.getStatus(),orderRequestDTO.getOrderId(),riderId);
        return "Message sent successfully for userId: " + orderRequestDTO.getUserId();
    }

    // 특정 주문 상태에 따른 내역 받기
    @GetMapping("/receive")
        public List<OrderResponseDTO> receiveMessages(@RequestParam String status) {
        return sqsService.getDeliveriesByStatus(status);
    }

    // 특정 주문 id에 따른 내역 받기
    @GetMapping("/orderId")
    public List<OrderResponseDTO> receiveOrderIdMessages(@RequestParam String orderId) {
        return sqsService.getOrdersByOrderId(orderId);
    }
 // 특정 주문번호와 상태에 따른 내역 받기
    @GetMapping("/orderIdStatus")
    public List<OrderResponseDTO> receiveOrderIdStatusMessages(@RequestParam String orderId, @RequestParam String status) {
        return sqsService.getOrderByStatusAndId(orderId,status);
    }
    //라이더에 따른 내역 받기
    @GetMapping("/getRiderOrders")
    public List<OrderResponseDTO> receiveAllOrderList(@RequestParam String riderId) {
        return sqsService.getOrdersByOrderId(riderId);
    }
    // 모든 레디스에 저장된 order 데이터 가져오기
    @GetMapping("/getAllOrder")
    public List<OrderResponseDTO> receiveAllOrderList(){
        return sqsService.getAllOrders();
    }
}
