package com.mongodb.test.model;

import org.bson.codecs.pojo.annotations.BsonId;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Account {
    @BsonId
    private int id;
    //private ObjectId id;
    private double balance;
    public Account(int id, double balance) {
        this.id = id;
        this.balance = balance;
    }
}
