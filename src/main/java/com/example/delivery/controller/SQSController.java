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
    // 유저아이디와 상태 따른 내역 받기
    @GetMapping("/userId")
    public List<OrderResponseDTO> receiveUserIdMessages(@RequestParam String userId, @RequestParam String status) {
        return sqsService.getOrdersByUserId(userId, status);
    }

    //라이더와 상태 따른 내역 받기
    @GetMapping("/getRiderOrders")
    public List<OrderResponseDTO> receiveRiderOrderList(@RequestParam String riderId, @RequestParam String status) {
        return sqsService.getDeliveriesByRiderId(riderId, status);
    }
    // 유저아이디  따른 내역 받기
    @GetMapping("/userIdLIst")
    public List<OrderResponseDTO>  receiveUserIdMessages(@RequestParam String userId) {
        return sqsService.getUserIdList(userId);
    }
// 유저아이디  따른 갯수 내역 받기
    @GetMapping("/userOrderCount")
    public int  getUserOrderCount(@RequestParam String userId,@RequestParam String role) {
        return sqsService.getUserOrderCount(userId,role);
    }
    // 유저아이디  따른 채팅리스트 갯수 내역 받기
    @GetMapping("/userChatCount")
    public int  getUserChatCount(@RequestParam String userId,@RequestParam String role) {
        return sqsService.getUserChatCount(userId,role);
    }
    //라이더와 주문선택 따른 내역 받기
    @GetMapping("/getRiderOrdersList")
    public List<OrderResponseDTO>  receiveCountChatList(@RequestParam String riderId) {
        return sqsService.getRiderIdList(riderId);
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
    public OrderResponseDTO receiveOrderIdStatusMessages(@RequestParam String orderId, @RequestParam String status) {
        return sqsService.getOrdersByStatusAndId(orderId,status);
    }

    // 모든 레디스에 저장된 order 데이터 가져오기
    @GetMapping("/getAllOrder")
    public List<OrderResponseDTO> receiveAllOrderList(){
        return sqsService.getAllOrders();
    }
    @GetMapping("/redisDelete")
    public void deleteRedis(){
        sqsService.deleteRedis();
        System.out.println("Redis 데이터베이스의 모든 키가 삭제되었습니다.");
    }
}
