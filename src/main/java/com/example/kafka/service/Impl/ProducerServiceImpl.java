package com.example.kafka.service.Impl;

import com.example.kafka.model.KafkaModel;
import com.example.kafka.service.api.ProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Component
public class ProducerServiceImpl implements ProducerService {
    @Autowired
    KafkaTemplate<String, KafkaModel> kafkaTemplate;

    public void sendMessage(String message) {
        String msgId = String.valueOf(System.currentTimeMillis());
        kafkaTemplate.send("mytopic",msgId,new KafkaModel(msgId,message)).addCallback(new ListenableFutureCallback<SendResult<String, KafkaModel>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("message error : "+message,ex);
            }

            @Override
            public void onSuccess(SendResult<String, KafkaModel> result) {
                log.info("message sent."+message+" - "+result.getRecordMetadata().partition()+"-"+result.getRecordMetadata().offset());
            }
        });
    }
}
