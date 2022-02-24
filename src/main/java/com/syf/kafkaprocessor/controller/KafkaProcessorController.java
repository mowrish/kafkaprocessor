package com.syf.kafkaprocessor.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.syf.kafkaprocessor.service.KafkaProcessorService;

@RestController
public class KafkaProcessorController {
	
	@Autowired
	private KafkaProcessorService processorServ;
	
	@GetMapping("/publish")
	public String sendMessage() {
		processorServ.sendMessage();
		return "message published successfully";
	}

}
