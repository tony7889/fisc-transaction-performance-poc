package com.mongodb.test.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.test.model.Stat;
import com.mongodb.test.service.AccountService;
import com.mongodb.test.service.AccountService.MODE;

@RestController
public class NoTransactionController {
    
    @Autowired
    private AccountService service;


    @GetMapping("/transfer")
    public ResponseEntity<Stat> transfer(@RequestParam(defaultValue = "false") boolean batch, @RequestParam(defaultValue = "false") boolean hasError, @RequestParam(required = false) String shard) {
        return new ResponseEntity<>(service.transferMultiple(MODE.NO_TRANSACTION, batch, hasError, shard), HttpStatus.OK);
    }


}
