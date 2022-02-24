package com.syf.kafkaprocessor.service;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaProcessorService {

	public void sendMessage() {
		
		Map<String, Object> configs = new HashMap<>();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configs.put(ProducerConfig.ACKS_CONFIG, "all");
		configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
		
		Map<String, Object> consumerConfigs = new HashMap<>();
		consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		
		Producer<String, String> producer = new KafkaProducer<>(configs);
		producer.send(new ProducerRecord<String, String>("request-topic", "2"));
		producer.close();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);
		consumer.subscribe(Arrays.asList("request-topic"));
		for (int i = 0; i < 5; i++) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	         for (ConsumerRecord<String, String> record : records)
	        	 System.out.println("custom - " + record.value());
		}
		consumer.unsubscribe();
		consumer.close();
		System.out.println("method execution completed");
	}

}
