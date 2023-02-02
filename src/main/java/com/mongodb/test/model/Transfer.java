package com.mongodb.test.model;

import java.util.List;

import lombok.Data;

@Data
public class Transfer {
    private Integer fromAccountId;
    private List<Integer> toAccountId;
}
