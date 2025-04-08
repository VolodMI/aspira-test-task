package com.task.leonparser.model;

import lombok.Data;
import java.util.List;

@Data
public class Match {
    private Long id;
    private String name;
    private Long kickoff;
    private List<Market> markets;
}
