package com.example.kafka.service.Impl;

import com.example.kafka.model.KafkaModel;
import com.example.kafka.model.MessageCover;
import com.example.kafka.service.api.ConsumerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;

@Slf4j
@Component
public class ConsumerServiceImpl implements ConsumerService {

    @KafkaListener(topics = "mytopic",errorHandler = "consumerErrorHandler")
    public void consumeMessage(@Payload MessageCover<?> messageCover,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                               @Header(KafkaHeaders.OFFSET) int offset,
                               @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey
                               /*Acknowledgment acknowledgment*/) {
        log.info("------received kafka message from consumer1-------");
        log.info("offset : "+offset);
        log.info("partition : "+partition);
        log.info("key : "+msgKey);
        LinkedHashMap body = (LinkedHashMap) messageCover.getBody();
        KafkaModel kafkaModel = new KafkaModel();
        kafkaModel.setMessage((String)body.get("message"));
        kafkaModel.setMessageId((String) body.get("messageId"));
        log.info("messageId : "+kafkaModel.getMessageId());
        log.info("message : "+kafkaModel.getMessage());
        if(messageCover.getMetadataList() != null){
            messageCover.getMetadataList().forEach(m-> log.info("k : "+m.getKey()+" -- v : "+m.getValue()));
        }
        //commit
        //acknowledgment.acknowledge();
    }

    @KafkaListener(topics = "mytopic2",errorHandler = "consumerErrorHandler")
    public void consumeMessage2(@Payload MessageCover<?> messageCover,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                               @Header(KafkaHeaders.OFFSET) int offset,
                               @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String msgKey) {
        log.info("------received kafka message from consumer2-------");
        log.info("offset : "+offset);
        log.info("partition : "+partition);
        log.info("key : "+msgKey);
        LinkedHashMap body = (LinkedHashMap) messageCover.getBody();
        KafkaModel kafkaModel = new KafkaModel();
        kafkaModel.setMessage((String)body.get("message"));
        kafkaModel.setMessageId((String) body.get("messageId"));
        log.info("messageId : "+kafkaModel.getMessageId());
        log.info("message : "+kafkaModel.getMessage());
        if(messageCover.getMetadataList() != null){
            messageCover.getMetadataList().forEach(m-> log.info("k : "+m.getKey()+" -- v : "+m.getValue()));
        }
    }
}
