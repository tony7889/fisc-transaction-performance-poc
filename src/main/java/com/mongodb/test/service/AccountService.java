package com.mongodb.test.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
import com.mongodb.test.model.Transfer;

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

    @Value("${settings.noOfThread}")
    private int noOfThread;

    @Value("${settings.transferAmount}")
    private int transferAmount;

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
        int pageSize = this.noOfAccount / this.noOfThread;
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
    public Stat transferMultiple(MODE mode, boolean isBatch) {
        return this.transferMultiple(mode, isBatch, false);
    }

    public Stat transferMultiple(MODE mode, boolean isBatch, boolean hasError) {
        Stat s = new Stat();
        StopWatch sw = new StopWatch();
        var ends = new ArrayList<CompletableFuture<StopWatch>>();
        List<Transfer> transfers = generateTransfer();

        int pageSize = transfers.size() / this.noOfThread;
        if (pageSize <= 0) {
            pageSize = 1;
        }
        int accPages = transfers.size() / pageSize;
        for (int pageIdx = 0; pageIdx <= accPages; pageIdx++) {
            int fromIdx = pageIdx * pageSize;
            int toIdx = Math.min(transfers.size(), (pageIdx + 1) * pageSize);
            var subList = transfers.subList(fromIdx, toIdx);

            switch (mode) {
                case NO_TRANSACTION:
                    ends.add(this.asyncService.transfer(subList, isBatch, hasError));
                    break;
                case CALLBACK:
                    ends.add(this.asyncService.callbackTransfer(subList, isBatch, hasError));
                    break;
                case CORE:
                    ends.add(this.asyncService.coreTransfer(subList, isBatch, hasError));
                    break;
            }

            if (toIdx == transfers.size()) {
                break;
            }
        }

        s.setOperation("transfer-update");
        s.setBatchSize(noOfTransfer * (transferAmount + 1));
        s.setStartAt(LocalDateTime.now());
        sw.start();
        CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
        sw.stop();
        s.setDuration(sw.getTotalTimeMillis());
        s.setEndAt(LocalDateTime.now());
        Document doc = database.getCollection(collectionName).aggregate(Arrays.asList(
                Aggregates.group("true", Accumulators.sum("total", "$balance")))).first();
        logger.info("End Batch, total amount in the world:" + doc.toString());
        return s;
    }

    public Stat longTransaction(long waitTime) throws InterruptedException {

        Stat s = new Stat();
        s.setOperation("longTransaction-update");
        s.setBatchSize(noOfTransfer * (transferAmount + 1));
        s.setStartAt(LocalDateTime.now());
        StopWatch sw = new StopWatch();
        sw.start();
        this.asyncService.longTransaction(waitTime, generateTransfer());
        sw.stop();
        s.setDuration(sw.getTotalTimeMillis());
        s.setEndAt(LocalDateTime.now());
        return s;
    }

    private List<Transfer> generateTransfer() {
        List<Transfer> transfers = new ArrayList<>();
        for (int i = 0; i < noOfTransfer; i++) {
            Transfer t = new Transfer();
            t.setFromAccountId((int) Math.floor(Math.random() * noOfAccount) + 1);
            for (int j = 0; j < transferAmount; j++) {
                if (t.getToAccountId() == null) {
                    t.setToAccountId(new ArrayList<>());
                }
                t.getToAccountId().add((int) Math.floor(Math.random() * noOfAccount) + 1);
            }
            transfers.add(t);
        }
        return transfers;
    }
}
