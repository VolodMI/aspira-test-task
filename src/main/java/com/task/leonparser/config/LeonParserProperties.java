package com.task.leonparser.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "leonparser")
public class LeonParserProperties {
    private List<String> targetSports;
    private int matchesLimit;
    private boolean topLeaguesOnly;
}
