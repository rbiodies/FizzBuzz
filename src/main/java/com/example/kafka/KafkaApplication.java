package com.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/*
 * Apache Kafka — это распределенная и отказоустойчивая система обработки потоков.
 * Start the ZooKeeper service
 * Start the Kafka broker service
 * Брокеры могут создавать кластер Kafka, обмениваясь информацией с помощью Zookeeper.
 * Брокер получает сообщения от производителей,
 * а потребители получают сообщения от брокера по теме, разделу и смещению
 */

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

        MessageProducer producer = context.getBean(MessageProducer.class);
        MessageListener listener = context.getBean(MessageListener.class);

        /*
         * Sending numbers messages to topic 'baeldung'.
         * Must be received by listener with group foo
         * and bar with containerFactory fooKafkaListenerContainerFactory.
         */
        for (int i = 1; i <= 1000000; i++) {
            producer.sendMessage(i);
        }
        /*
         * В первой форме ожидание длится до тех пор, пока отсчет,
         * связанный с вызывающим объектом типа CountDownLatch, не достигнет нуля.
         * А во второй форме ожидание длится только в течение определенного периода времени,
         * определяемого параметром ожидание.
         */
        listener.latch.await(10, TimeUnit.SECONDS);

        context.close();
    }

    @Bean
    public MessageProducer messageProducer() {
        return new MessageProducer();
    }

    @Bean
    public MessageListener messageListener() {
        return new MessageListener();
    }

    // Мы можем отправлять сообщения с помощью класса KafkaTemplate
    public static class MessageProducer {

        @Autowired
        private KafkaTemplate<String, Integer> kafkaTemplate;

        @Value(value = "${message.topic.name}")
        private String topicName;

        public void sendMessage(Integer message) {

            kafkaTemplate.send(topicName, message);
        }
    }

    public static class MessageListener {

        /*
         * Объект этого класса изначально создается с количеством событий,
         * которые должны произойти до того момента, как будет снята самоблокировка.
         * Всякий раз, когда происходит событие, значение счетчика уменьшается
         */
        private final CountDownLatch latch = new CountDownLatch(1000000);

        String s = "";
        int c3, c5;

        /*
         * Использование сообщений
         * Оптимизация задачи Fizz Buzz
         * Оператор по модулю является очень дорогостоящей операцией по сравнению с другими арифметическими операциями,
         * а i%15 интерпретируется как i%3 и i%5,
         * следовательно, это дорогостоящий способ решения проблемы с точки зрения временной сложности.
         * Мы можем решить то же самое, используя простые операции сложения,
         * пожертвовав дополнительным пространством переменных
         */
        @KafkaListener(topics = "${message.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
        public void listenGroupFoo(String message) {
            c3++;
            c5++;
            if (c3 == 3) {
                s += "fizz";
                c3 = 0;
            }
            if (c5 == 5) {
                s += "buzz";
                c5 = 0;
            }
            if (s.isEmpty()) {
                System.out.println(message);
            } else {
                System.out.println(s);
            }
            s = "";
            // Всякий раз, когда вызывается метод countDown(), отсчет, связанный с вызывающим объектом, уменьшается на единицу.
            latch.countDown();
        }
    }
}

/*
 * Каждый топик может иметь 1 или больше разделов, распараллеленных на разные узлы кластера (брокеры),
 * чтобы сразу несколько потребителей могли считывать данные из одного топика одновременно;
 */
