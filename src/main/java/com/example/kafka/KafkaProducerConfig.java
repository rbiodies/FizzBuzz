package com.example.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

// Производитель — это клиент, который отправляет сообщения на сервер Kafka в указанную тему
@Configuration
public class KafkaProducerConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    /*
     * Чтобы создавать сообщения, нам сначала нужно настроить ProducerFactory .
     * Это задает стратегию создания экземпляров Kafka Producer
     * BOOTSTRAP_SERVERS_CONFIG- Хост и порт, на котором работает Kafka.
     * KEY_SERIALIZER_CLASS_CONFIG- Класс сериализатора, который будет использоваться для ключа.
     * VALUE_SERIALIZER_CLASS_CONFIG- Класс сериализатора, который будет использоваться для значения.
     * Мы используем StringSerializer для ключей и IntegerSerializer для значений.
     */
    @Bean
    public ProducerFactory<String, Integer> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapAddress);
        configProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    // KafkaTemplate обертывает экземпляр Producer и предоставляет удобные методы для отправки сообщений в темы Kafka
    @Bean
    public KafkaTemplate<String, Integer> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
