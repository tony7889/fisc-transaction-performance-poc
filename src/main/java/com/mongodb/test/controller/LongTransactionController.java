package com.mongodb.test.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.test.model.Stat;
import com.mongodb.test.service.AccountService;

@RestController
public class LongTransactionController {
    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    @Autowired
    private AccountService service;


    @GetMapping("/long/transaction")
    public ResponseEntity<Stat> longTransaction() throws InterruptedException {
        return new ResponseEntity<>(this.service.longTransaction(30000l), HttpStatus.OK);
    }

    @GetMapping("/long/transaction/timeout")
    public ResponseEntity<Stat> longTransactionTimeout() throws InterruptedException {
        return new ResponseEntity<>(this.service.longTransaction(80000l), HttpStatus.OK);
    }


}
