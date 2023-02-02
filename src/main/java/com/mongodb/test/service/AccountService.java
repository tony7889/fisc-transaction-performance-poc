package com.mongodb.test.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.test.model.Account;
import com.mongodb.test.model.Stat;

@Service
public class AccountService {
    public static enum MODE {
        NO_TRANSACTION,
        CALLBACK,
        CORE
    };

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${settings.collectionName}")
    private String collectionName;

    @Value("${settings.noOfAccount}")
    private int noOfAccount;

    @Value("${settings.noOfTransfer}")
    private int noOfTransfer;

    @Value("${settings.initialBalance}")
    private int initialBalance;

    @Autowired
    private MongoDatabase database;

    @Autowired
    private AsyncAccountService asyncService;

    public Stat init() throws InterruptedException {
        Stat s = new Stat();
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        collection.drop();
        var accounts = new ArrayList<Account>();
        for (int i = 0; i < this.noOfAccount; i++) {
            accounts.add(new Account(i + 1, initialBalance));
        }
        var ends = new ArrayList<CompletableFuture<StopWatch>>();
        int pageSize = this.noOfAccount / 10;
        int accPages = accounts.size() / pageSize;
        for (int pageIdx = 0; pageIdx <= accPages; pageIdx++) {
            int fromIdx = pageIdx * pageSize;
            int toIdx = Math.min(accounts.size(), (pageIdx + 1) * pageSize);
            var subList = accounts.subList(fromIdx, toIdx);

            ends.add(this.asyncService.insertMany(collection, subList));

            if (toIdx == accounts.size()) {
                break;
            }
        }
        // CompletableFuture.allOf(ends.toArray(new
        // CompletableFuture[ends.size()])).join();

        s.setOperation("init-insert");
        s.setBatchSize(accounts.size());
        s.setStartAt(LocalDateTime.now());
        ends.stream().map(CompletableFuture::join).forEach((sw) -> {
            s.setDuration(sw.getTotalTimeMillis());
        });
        s.setEndAt(LocalDateTime.now());

        return s;
    }

    public Stat transfer(MODE mode, boolean isBatch) {
        Stat s = new Stat();
        var ends = new ArrayList<CompletableFuture<StopWatch>>();
        for (int i = 0; i < noOfTransfer; i++) {
            switch (mode) {
                case NO_TRANSACTION:
                    ends.add(this.asyncService.transfer(isBatch));
                    break;
                case CALLBACK:
                    ends.add(this.asyncService.callbackTransfer(isBatch));
                    break;
                case CORE:
                    ends.add(this.asyncService.coreTransfer(isBatch));
                    break;
            }
        }

        s.setOperation("transfer-update");
        s.setBatchSize(noOfTransfer*51);
        s.setStartAt(LocalDateTime.now());
        ends.stream().map(CompletableFuture::join).forEach((sw) -> {
            s.setDuration(sw.getTotalTimeMillis());
        });
        s.setEndAt(LocalDateTime.now());
        //CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
        Document doc = database.getCollection(collectionName).aggregate(Arrays.asList(
                Aggregates.group("true", Accumulators.sum("total", "$balance")))).first();
        logger.info("End Batch, total amount in the world:" + doc.toString());
        return s;
    }
}
