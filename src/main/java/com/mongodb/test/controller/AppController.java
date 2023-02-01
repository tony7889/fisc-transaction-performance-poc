package com.mongodb.test.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

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
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.test.model.Account;
import com.mongodb.test.service.AccountService;

@RestController
public class AppController {
    @Autowired
    private MongoClient client;
    @Autowired
    private MongoDatabase database;
    @Autowired
    private AccountService service;

    @Value("${settings.dbName}")
    private String dbName;
    @Value("${settings.collectionName}")
    private String collectionName;

    @Value("${settings.noOfAccount}")
    private int noOfAccount;

    @Value("${settings.noOfTransfer}")
    private int noOfTransfer;

    @Value("${settings.noOfThread}")
    private int noOfThread;
    @Value("${settings.initialBalance}")
    private int initialBalance;

    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    @GetMapping("/init")
    public ResponseEntity<String> startInit() throws InterruptedException {
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        collection.drop();
        var accounts = new ArrayList<Account>();
        for (int i = 0; i < this.noOfAccount; i++) {
            accounts.add(new Account(i + 1, initialBalance));
            // collection.insertOne(a);
        }
        var ends = new ArrayList<CompletableFuture<Void>>();
        int pageSize = this.noOfAccount / this.noOfThread;
        int accPages = accounts.size() / pageSize;
        for (int pageIdx = 0; pageIdx <= accPages; pageIdx++) {
            int fromIdx = pageIdx * pageSize;
            int toIdx = Math.min(accounts.size(), (pageIdx + 1) * pageSize);
            var subList = accounts.subList(fromIdx, toIdx);
            ends.add(this.service.asyncInsertAccount(collection, subList));

            if (toIdx == accounts.size()) {
                break;
            }
        }
        CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
        return new ResponseEntity<>("Init success", HttpStatus.OK);
    }

    @GetMapping("/transfer")
    public ResponseEntity<String> transfer() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                runBatchTransfer();
            }

        }).start();

        return new ResponseEntity<>("Transfer running in background", HttpStatus.OK);
    }

    @GetMapping("/callback/transfer")
    public ResponseEntity<String> transferWithCallbackTransaction() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                final ClientSession clientSession = client.startSession();
                TransactionOptions txnOptions = TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .readConcern(ReadConcern.LOCAL)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build();
                TransactionBody<String> txnBody = new TransactionBody<String>() {
                    public String execute() {
                        runBatchTransfer(clientSession);
                        return "";
                    }
                };
                try {
                    clientSession.withTransaction(txnBody, txnOptions);
                } catch (RuntimeException e) {
                    logger.error("Error during transfer", e);
                } finally {
                    clientSession.close();
                }
            }

        }).start();

        return new ResponseEntity<>("Transfer running in background", HttpStatus.OK);
    }

    @GetMapping("/long/transaction")
    public ResponseEntity<String> longTransaction() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    TransactionOptions txnOptions = TransactionOptions.builder()
                            .readPreference(ReadPreference.primary())
                            .readConcern(ReadConcern.MAJORITY)
                            .writeConcern(WriteConcern.MAJORITY)
                            .build();
                    try (ClientSession clientSession = client.startSession()) {
                        clientSession.startTransaction(txnOptions);
                        logger.info("Start Transaction");
                        runBatchTransfer(clientSession);
                        logger.info("Start waiting for commit");
                        // Thread.sleep(10000l);
                        Thread.sleep(80000l);
                        clientSession.commitTransaction();
                        logger.info("Transaction committed");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        return new ResponseEntity<>("Long transaction running in background", HttpStatus.OK);
    }

    @GetMapping("/core/transfer")
    public ResponseEntity<String> transferWithCoreTransaction() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TransactionOptions txnOptions = TransactionOptions.builder()
                                .readPreference(ReadPreference.primary())
                                .readConcern(ReadConcern.MAJORITY)
                                .writeConcern(WriteConcern.MAJORITY)
                                .build();
                        try (ClientSession clientSession = client.startSession()) {
                            clientSession.startTransaction(txnOptions);
                            logger.info("Start Transaction");
                            runBatchTransfer(clientSession);
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
                            continue;
                        } else {
                            throw e;
                        }
                    }
                }

            }

        }).start();

        return new ResponseEntity<>("Transfer running in background", HttpStatus.OK);
    }

    public void runBatchTransfer() {
        this.runBatchTransfer(null);
    }

    public void runBatchTransfer(ClientSession clientSession) {
        var ends = new ArrayList<CompletableFuture<Void>>();
        for (int i = 0; i < noOfTransfer; i++) {
            ends.add(this.service.asyncRunTransfer(clientSession));
        }
        try {
            CompletableFuture.allOf(ends.toArray(new CompletableFuture[ends.size()])).join();
        } catch (CompletionException ex) {
            try {
                throw ex.getCause();
            } catch (MongoException possible) {
                // logger.error("retry", possible);
                throw possible;
            } catch (Throwable impossible) {
                throw new AssertionError(impossible);
            }
        }
        Document doc;
        if (clientSession != null)
            doc = database.getCollection(collectionName).aggregate(clientSession, Arrays.asList(
                    Aggregates.group("true", Accumulators.sum("total", "$balance")))).first();
        else
            doc = database.getCollection(collectionName).aggregate(Arrays.asList(
                    Aggregates.group("true", Accumulators.sum("total", "$balance")))).first();
        logger.info("End Batch, total amount in the world:" + doc.toString());
    }
}
