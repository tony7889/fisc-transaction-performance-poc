package com.mongodb.test.service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.test.model.Account;
import com.mongodb.test.model.Stat;
import com.mongodb.test.model.Transfer;

@Service
public class AsyncAccountService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoClient client;

    @Autowired
    private MongoDatabase database;

    @Value("${settings.collectionName}")
    private String collectionName;

    @Async
    public CompletableFuture<StopWatch> insertMany(MongoCollection<Account> collection, List<Account> accounts)
            throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        StopWatch sw = new StopWatch();
        sw.start();
        collection.insertMany(accounts);
        sw.stop();
        logger.info(Thread.currentThread().getName() + " " + accounts.size() + " inserted. takes "
                + sw.getTotalTimeMillis() + "ms");
        return CompletableFuture.completedFuture(sw);
    }

    @Async
    public CompletableFuture<StopWatch> callbackTransfer(List<Transfer> transfers, boolean isBatch) {
        StopWatch sw = null;
        final ClientSession clientSession = client.startSession();
        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.MAJORITY)
                .build();
        TransactionBody<StopWatch> txnBody = new TransactionBody<StopWatch>() {
            public StopWatch execute() {
                return isBatch?transferBatch(clientSession, transfers):transfer(clientSession, transfers);
            }
        };
        try {
            sw = clientSession.withTransaction(txnBody, txnOptions);
        } catch (RuntimeException e) {
            logger.error("Error during transfer", e);
        } finally {
            clientSession.close();
        }
        return CompletableFuture.completedFuture(sw);
    }

    @Async
    public CompletableFuture<StopWatch> coreTransfer(List<Transfer> transfers, boolean isBatch) {
        StopWatch sw;
        while (true) {
            try {
                TransactionOptions txnOptions = TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .readConcern(ReadConcern.MAJORITY)
                        // .readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build();
                try (ClientSession clientSession = client.startSession()) {
                    clientSession.startTransaction(txnOptions);
                    logger.info("Start Transaction");
                    sw = isBatch?this.transferBatch(clientSession, transfers):this.transfer(clientSession, transfers);
                    while (true) {
                        try {
                            clientSession.commitTransaction();
                            logger.info("Transaction committed");
                            break;
                        } catch (MongoException e) {
                            // can retry commit
                            if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                logger.info("UnknownTransactionCommitResult, retrying commit operation ...");
                                continue;
                            } else {
                                logger.info("Exception during commit ...");
                                throw e;
                            }
                        }
                    }
                }
                break;
            } catch (MongoException e) {
                logger.info("Transaction aborted. Caught exception during transaction.");
                if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    // e.printStackTrace();
                    logger.info("TransientTransactionError, aborting transaction and retrying ...");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    continue;
                } else {
                    throw e;
                }
            }
        }
        return CompletableFuture.completedFuture(sw);
    }

    @Async
    public CompletableFuture<StopWatch> transfer(List<Transfer> transfers, boolean isBatch) {
        return CompletableFuture.completedFuture(isBatch?this.transferBatch(null, transfers):this.transfer(null, transfers));
    }

    private StopWatch transfer(ClientSession clientSession, List<Transfer> transfers) {
        StopWatch sw = new StopWatch();
        sw.start();
        for(Transfer t: transfers){
            int transferAmount = t.getToAccountId().size();
            MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
            logger.info(Thread.currentThread().getName() + " Deduct $" + transferAmount + " from account " + t.getFromAccountId());
            if (clientSession != null)
                collection.updateOne(clientSession, Filters.eq("_id", t.getFromAccountId()),
                        Updates.inc("balance", -transferAmount));
            else
                collection.updateOne(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -transferAmount));
    
            logger.info(Thread.currentThread().getName() + " Start transfering $1x50 to other account");
            for (Integer id2 : t.getToAccountId()) {
                if (clientSession != null)
                    collection.updateOne(clientSession, Filters.eq("_id", id2), Updates.inc("balance", 1));
                else
                    collection.updateOne(Filters.eq("_id", id2), Updates.inc("balance", 1));
            }
            sw.stop();
            logger.info("Completed transfer $1x50 from " + t.getFromAccountId() + " to " + Arrays.toString(t.getToAccountId().toArray()) + ". takes "
                    + sw.getTotalTimeMillis() + "ms");
        }
        return sw;
    }

    private StopWatch transferBatch(ClientSession clientSession, List<Transfer> transfers) {

        StopWatch sw = new StopWatch();
        sw.start();
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        List<UpdateOneModel<Account>> list = new ArrayList<>();
        for(Transfer t: transfers){
            int transferAmount = t.getToAccountId().size();
            logger.info(Thread.currentThread().getName() + " Deduct $" + transferAmount + " from account " + t.getFromAccountId());
            list.add(new UpdateOneModel<>(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -transferAmount)));
            logger.info(Thread.currentThread().getName() + " Start transfering $1x50 to other account");
            for (Integer id2 : t.getToAccountId()) {
                list.add(new UpdateOneModel<>(Filters.eq("_id", id2), Updates.inc("balance", 1)));
            }
        }

        if (clientSession != null)
            collection.bulkWrite(clientSession, list);
        else
            collection.bulkWrite(list);
        sw.stop();
        logger.info("Completed "+transfers.size()+" transfers. takes "
                + sw.getTotalTimeMillis() + "ms");
        return sw;
    }

    public Stat longTransaction(long waitTime, List<Transfer> transfers) throws InterruptedException {

        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .build();
        try (ClientSession clientSession = client.startSession()) {
            clientSession.startTransaction(txnOptions);
            logger.info("Start Transaction");
            transfer(clientSession, transfers);
            logger.info("Start waiting for commit");
            Thread.sleep(waitTime);
            clientSession.commitTransaction();
            logger.info("Transaction committed");
        }
        return new Stat();
    }
}
