package com.mogan.ecommerce;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        var producer = new KafkaProducer<String, String>(properties());
        var value = "123,1234,849490";
        var newOrder = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);

        var email = "Thank you for you order! We are processing you order";
        var sendEmail = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", email, email);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };

        producer.send(newOrder, callback).get();
        producer.send(sendEmail, callback).get();
    }

    private static Properties properties() {

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
