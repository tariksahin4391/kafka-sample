package com.example.kafka.service.Impl;

import com.example.kafka.model.KafkaModel;
import com.example.kafka.model.KafkaModel2;
import com.example.kafka.service.api.ConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerServiceImpl implements ConsumerService {

    @KafkaListener(topics = "mytopic",errorHandler = "consumerErrorHandler")
    public void consumeMessage(@Payload KafkaModel kafkaModel,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                               @Header(KafkaHeaders.OFFSET) int offset,
                               @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey) {
        log.info("------received kafka message-------");
        log.info("offset : "+offset);
        log.info("partition : "+partition);
        log.info("key : "+msgKey);
        log.info("messageId : "+kafkaModel.getMessageId());
        log.info("message : "+kafkaModel.getMessage());
    }
}
