package com.mongodb.test.controller;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class CallbackAPIController {
    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    @Autowired
    private AccountService service;


    @GetMapping("/callback/transfer")
    public ResponseEntity<Stat> transferWithCallbackAPITransaction(@RequestParam(defaultValue = "false") boolean batch, @RequestParam(defaultValue = "false") boolean hasError, @RequestParam(required = false) String shard) {
        return new ResponseEntity<>(service.transferMultiple(MODE.CALLBACK, batch, hasError, shard), HttpStatus.OK);
    }
}
