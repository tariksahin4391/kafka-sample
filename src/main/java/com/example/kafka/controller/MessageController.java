package com.example.kafka.controller;

import com.example.kafka.service.api.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
public class MessageController {
    @Autowired
    ProducerService producerService;

    @GetMapping("/send-message/{msg}")
    public ResponseEntity<String> sendMessage(@PathVariable("msg") String msg){
        producerService.sendMessage(msg);
        return ResponseEntity.status(HttpStatus.OK).body("OK");
    }
}
