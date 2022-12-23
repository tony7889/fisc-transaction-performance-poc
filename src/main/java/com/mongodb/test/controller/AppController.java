package com.mongodb.test.controller;

import java.util.Arrays;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.test.model.Account;

@RestController
public class AppController {
    @Autowired
    private MongoClient client;
    @Autowired
    private MongoDatabase database;

    @Value("${settings.dbName}")
    private String dbName;
    @Value("${settings.collectionName}")
    private String collectionName;

    @Value("${settings.noOfAccount}")
    private int noOfAccount;
    @Value("${settings.initialBalance}")
    private int initialBalance;
    @Value("${settings.noOfTransfer}")
    private int noOfTransfer;
    @Value("${settings.transferAmount}")
    private int transferAmount;

    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    @GetMapping("/init")
    public ResponseEntity<String> startInsertOdd() {
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        collection.drop();
        for (int i = 0; i < this.noOfAccount; i++) {
            Account a = new Account(i + 1, initialBalance);
            collection.insertOne(a);
        }
        return new ResponseEntity<>("Init success", HttpStatus.OK);
    }

    @GetMapping("/transfer")
    public ResponseEntity<String> transfer() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                runTransfer();
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
                /* Step 2: Optional. Define options to use for the transaction. */
                TransactionOptions txnOptions = TransactionOptions.builder()
                        .readPreference(ReadPreference.primary())
                        .readConcern(ReadConcern.LOCAL)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build();
                /*
                 * Step 3: Define the sequence of operations to perform inside the transactions.
                 */
                TransactionBody txnBody = new TransactionBody<String>() {
                    public String execute() {
                        runTransfer(clientSession);
                        return "";
                    }
                };
                try {
                    /*
                     * Step 4: Use .withTransaction() to start a transaction,
                     * execute the callback, and commit (or abort on error).
                     */
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
                        runTransfer(clientSession);
                        logger.info("Start waiting for commit");
                        //Thread.sleep(10000l);
                        Thread.sleep(70000l);
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
                            runTransfer(clientSession);
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

    private void runTransfer() {
        runTransfer(null);
    }

    private void runTransfer(ClientSession clientSession) {
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        for (int i = 0; i < noOfTransfer; i++) {
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
            Document doc;
            if (clientSession != null)
                doc = database.getCollection(collectionName).aggregate(clientSession, Arrays.asList(
                        Aggregates.group("true", Accumulators.sum("total", "$balance")))).first();
            else
                doc = database.getCollection(collectionName).aggregate(Arrays.asList(
                        Aggregates.group("true", Accumulators.sum("total", "$balance")))).first();
            logger.info(doc.toString());
        }
    }
}
