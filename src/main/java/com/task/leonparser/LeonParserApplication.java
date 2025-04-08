package com.task.leonparser;

import com.task.leonparser.service.LeonService;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class LeonParserApplication {
    public static void main(String[] args) {
        SpringApplication.run(LeonParserApplication.class, args);
    }

    @Bean
    public ApplicationRunner run(LeonService leonService) {
        return args -> leonService.startParsing();
    }
}
