package com.task.leonparser.model;

import lombok.Data;
import java.util.List;

@Data
public class Market {
    private Long id;
    private String name;
    private List<Runner> runners;
}
