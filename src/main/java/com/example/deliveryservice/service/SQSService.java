package com.example.deliveryservice.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class SQSService {

    private final SqsClient sqsClient;

    @Value("${spring.cloud.aws.sqs.queue-url}")
    private String queueUrl; // YML에서 주입받은 queueUrl

    // 메시지 전송
    public void sendMessage(String userId, String message) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .messageGroupId(userId)  // FIFO 큐의 messageGroupId 설정
                .messageDeduplicationId(UUID.randomUUID().toString())  // 메시지 중복 제거 ID
                .build();

        SendMessageResponse response = sqsClient.sendMessage(sendMessageRequest);
        System.out.println("Message sent with ID: " + response.messageId());
    }
    // 메시지 받기 (모든 메시지 받기)
    public List<String> receiveAllMessages() {
        // SQS에서 메시지 요청
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)  // 큐 URL
                .maxNumberOfMessages(10)  // 최대 10개 메시지 가져오기
                .waitTimeSeconds(20)  // 대기 시간
                .build();

        // 메시지 수신
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

        // 모든 메시지 내용만 반환 (MessageGroupId와 상관없이)
        return messages.stream()
                .map(Message::body) // 메시지 내용만 반환
                .collect(Collectors.toList());
    }
    // 메시지 받기 (특정 MessageGroupId로 필터링)
    public List<Message> receiveMessages(String userId) {
        // SQS에서 메시지 요청
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)  // 큐 URL
                .maxNumberOfMessages(10)  // 최대 10개 메시지 가져오기
                .waitTimeSeconds(20)  // 대기 시간
                .build();

        // 메시지 수신
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

        // 메시지 필터링 (MessageGroupId가 userId와 일치하는 메시지만 반환)
        return messages.stream()
                .filter(message -> {
                    // messageGroupId는 message 속성에서 직접 가져옵니다.
                    String messageGroupId = message.attributes().get("MessageGroupId");
                    return userId.equals(messageGroupId);  // userId와 비교
                })
                .collect(Collectors.toList());
    }

    // 메시지 삭제 (특정 userId에 속한 모든 메시지 삭제)
    public void deleteMessagesByUserId(String userId) {
        // userId에 속한 메시지들 받기
        List<Message> messages = receiveMessages(userId);

        // 해당 메시지들을 순차적으로 삭제
        for (Message message : messages) {
            String receiptHandle = message.receiptHandle();

            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            // 메시지 삭제
            sqsClient.deleteMessage(deleteMessageRequest);
            System.out.println("Message deleted with ReceiptHandle: " + receiptHandle);
        }
    }

}
