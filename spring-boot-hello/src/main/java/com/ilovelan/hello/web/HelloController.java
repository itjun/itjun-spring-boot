package com.ilovelan.hello.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    @Value("${server.port}")
    private String port;

    @RequestMapping("/hello")
    public String hello() {
        System.out.println(this.port);
        return "Hello, Spring Boot 2.0 !";
    }
}
