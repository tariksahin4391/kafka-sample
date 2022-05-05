package com.example.kafka.config;

import com.example.kafka.model.KafkaModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${kafkaBootstrapAddress}")
    private String bootstrapAddress;

    @Value(value = "${kafkaGroupId}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, KafkaModel> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,3);
        return new DefaultKafkaConsumerFactory<>(props,new StringDeserializer(),
                new JsonDeserializer<>(KafkaModel.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaModel> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaModel> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerErrorHandler consumerErrorHandler(){
        return new ConsumerErrorHandler();
    }
}
