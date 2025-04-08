package com.task.leonparser.model;

import lombok.Data;
import java.util.List;

@Data
public class Region {
    private Long id;
    private String name;
    private List<League> leagues;
}
