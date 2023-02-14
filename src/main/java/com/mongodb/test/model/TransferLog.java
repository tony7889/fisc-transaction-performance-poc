package com.mongodb.test.model;

import java.util.Date;

import lombok.Data;

@Data
public class TransferLog {
    private double amount;
    private Integer fromAccountId;
    private Integer toAccountId;
    private Date cAt;
    public TransferLog(double amount, Integer fromAccountId, Integer toAccountId) {
        this.amount = amount;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.cAt = new Date();
    }
}
