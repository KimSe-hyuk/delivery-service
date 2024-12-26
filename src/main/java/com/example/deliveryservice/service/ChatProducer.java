package com.example.deliveryservice.service;

import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ChatProducer {

    private final SqsClient sqsClient;

    @Value("${spring.cloud.aws.sqs.queue-url}")
    private String queueUrl;

    public ChatProducer(SqsClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    // 메시지 전송 메서드
    public void sendMessage(String userId, String productId, String role, String message) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .messageGroupId(productId)  // FIFO 큐에서 제품 ID로 그룹화하여 순서 보장
                .messageAttributes(Map.of(
                        "userId", MessageAttributeValue.builder().dataType("String").stringValue(userId).build(),
                        "productId", MessageAttributeValue.builder().dataType("String").stringValue(productId).build(),
                        "role", MessageAttributeValue.builder().dataType("String").stringValue(role).build()))
                .build();

        // 메시지 전송
        sqsClient.sendMessage(sendMessageRequest);
    }

    // 메시지 받기 메서드
    public List<String> receiveMessages() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)  // 긴 폴링 설정
                .messageAttributeNames("All")  // 모든 메시지 속성 가져오기
                .build();

        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        List<String> messageList = new ArrayList<>();

        // 받은 메시지 처리
        for (Message message : messages) {
            String userId = message.messageAttributes().get("userId").stringValue();
            String productId = message.messageAttributes().get("productId").stringValue();
            String role = message.messageAttributes().get("role").stringValue();
            String messageBody = message.body();

            // 메시지 출력 (채팅 메시지 출력 예시)
            messageList.add("User: " + userId + " (" + role + ") says about Product " + productId + ": " + messageBody);

            // 메시지 삭제 (처리 완료 후)
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
        }

        return messageList;
    }
}
