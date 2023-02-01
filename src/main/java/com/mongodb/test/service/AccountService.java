package com.mongodb.test.service;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.test.model.Account;

@Service
public class AccountService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoDatabase database;

    @Value("${settings.collectionName}")
    private String collectionName;

    @Value("${settings.noOfAccount}")
    private int noOfAccount;

    @Value("${settings.transferAmount}")
    private int transferAmount;

    @Async
    public CompletableFuture<Void> asyncInsertAccount(MongoCollection<Account> collection, List<Account> accounts)
            throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        StopWatch sw = new StopWatch();
        sw.start();
        collection.insertMany(accounts);
        sw.stop();
        logger.info(Thread.currentThread().getName() + " " + accounts.size() + " inserted at "
                + LocalDateTime.now().toString() + ". takes " + sw.getTotalTimeMillis() + "ms");
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> asyncRunTransfer(ClientSession clientSession) {
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        int randomId = (int) Math.floor(Math.random() * noOfAccount) + 1;
        logger.info("Deduct $" + transferAmount + " from account " + randomId);
        if (clientSession != null)
            collection.updateOne(clientSession, Filters.eq("_id", randomId),
                    Updates.inc("balance", -transferAmount));
        else
            collection.updateOne(Filters.eq("_id", randomId), Updates.inc("balance", -transferAmount));
        int[] to = new int[transferAmount];

        logger.info("Start transfering $1x50 to other account");
        for (int j = 0; j < transferAmount; j++) {
            int randomId2 = (int) Math.floor(Math.random() * noOfAccount) + 1;
            if (clientSession != null)
                collection.updateOne(clientSession, Filters.eq("_id", randomId2), Updates.inc("balance", 1));
            else
                collection.updateOne(Filters.eq("_id", randomId2), Updates.inc("balance", 1));
            to[j] = randomId2;
        }
        logger.info("Completed transfer $1x50 from " + randomId + " to " + Arrays.toString(to));
        return CompletableFuture.completedFuture(null);
    }
}
