package com.example.demo.controller;

import com.example.demo.config.KafkaConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MyController {
  @Autowired
  private KafkaConfig conf;

  @RequestMapping("/hi")
  public String hi() {
    return "hi man " + conf;
  }

}