package com.example.kafka.service.Impl;

import com.example.kafka.model.KafkaModel;
import com.example.kafka.model.KafkaModel2;
import com.example.kafka.model.MessageCover;
import com.example.kafka.model.MessageMetadata;
import com.example.kafka.service.api.ProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class ProducerServiceImpl implements ProducerService {
    @Autowired
    KafkaTemplate<String, MessageCover<?>> kafkaTemplate;

    public void sendMessage(String message) {
        String msgId = String.valueOf(System.currentTimeMillis());
        List<MessageMetadata> metadataList = new ArrayList<>();
        metadataList.add(new MessageMetadata("msgId",msgId));
        MessageCover<KafkaModel> newMessage = new MessageCover<>();
        newMessage.setMetadataList(metadataList);
        KafkaModel2 m2 = new KafkaModel2();
        m2.setLongVal(3L);
        m2.setStrVal("str val");
        newMessage.setBody(new KafkaModel(msgId,message,m2));
        kafkaTemplate.send("mytopic",msgId,newMessage).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("message error : " + message, ex);
            }

            @Override
            public void onSuccess(SendResult<String, MessageCover<?>> result) {
                log.info("message sent." + message + " - " + result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
            }
        });
    }
}
