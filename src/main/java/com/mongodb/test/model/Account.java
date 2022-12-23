package com.mongodb.test.model;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import lombok.Data;

@Data
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
