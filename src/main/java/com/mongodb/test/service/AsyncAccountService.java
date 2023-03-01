package com.mongodb.test.service;

import java.time.LocalDateTime;
import java.util.*;
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
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.test.model.Account;
import com.mongodb.test.model.Transfer;
import com.mongodb.test.model.TransferLog;

@Service
public class AsyncAccountService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MongoClient client;

    @Autowired
    private MongoDatabase database;

    @Value("${settings.collectionName}")
    private String collectionName;

    @Value("${settings.transferLogCollectionName}")
    private String transferLogCollectionName;

    @Async
    public CompletableFuture<StopWatch> insertMany(MongoCollection<Account> collection, List<Account> accounts)
            throws InterruptedException {
        logger.info(Thread.currentThread().getName() + " start at: " + LocalDateTime.now().toString());
        StopWatch sw = new StopWatch();
        sw.start();
        collection.insertMany(accounts);
        sw.stop();
        //logger.info(Thread.currentThread().getName() + " " + accounts.size() + " inserted. takes "
        //        + sw.getTotalTimeMillis() + "ms, TPS:" + accounts.size() / sw.getTotalTimeSeconds());
        return CompletableFuture.completedFuture(sw);
    }

    @Async
    public CompletableFuture<Void> callbackTransfer(List<Transfer> transfers, boolean isBatch, boolean hasError, String shard) {
        for (Transfer transfer : transfers) {
            final ClientSession clientSession = client.startSession();
            TransactionOptions txnOptions = TransactionOptions.builder()
                    .readPreference(ReadPreference.primary())
                    .readConcern(ReadConcern.MAJORITY)
                    .writeConcern(WriteConcern.MAJORITY)
                    .build();
            TransactionBody<Void> txnBody = new TransactionBody<Void>() {
                public Void execute() {
                    if (isBatch) {
                        transferBatch(clientSession, transfer, shard); 
                    }else {
                        transfer(clientSession, transfer, hasError, shard);
                    }
                    return null;
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
        return CompletableFuture.completedFuture(null);

    }

    @Async
    public CompletableFuture<Void> coreTransfer(List<Transfer> transfers, boolean isBatch, boolean hasError, String shard) {
        for (Transfer transfer : transfers) {

            UUID tranId = null;
            int retryCount = 0;
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
                        tranId = clientSession.getServerSession().getIdentifier().getBinary("id").asUuid();
                        logger.info("Start Transaction: " + tranId);
                        if (isBatch) {
                            this.transferBatch(clientSession, transfer, shard); 
                        }else {
                            this.transfer(clientSession, transfer, hasError, shard);
                        }
                        while (true) {
                            try {
                                clientSession.commitTransaction();
                                logger.info("Transaction committed: " + tranId);
                                break;
                            } catch (MongoException e) {
                                // can retry commit
                                if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                    logger.info("UnknownTransactionCommitResult, retrying " + tranId + " commit operation ...");
                                    retryCount++;
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
                    //logger.info("Transaction aborted. Caught exception during transaction.");
                    if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                        // e.printStackTrace();
                        logger.info("TransientTransactionError, aborting transaction " + tranId + " and retrying ...");
                        retryCount++;
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
            logger.info("Retry count:" + retryCount);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> transfer(List<Transfer> transfers, boolean isBatch, boolean hasError, String shard) {
        for (Transfer transfer : transfers) {
            if (isBatch) {
                this.transferBatch(null, transfer, shard); 
            }else {
                this.transfer(null, transfer, hasError, shard);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async
    public CompletableFuture<Void> transfer(Transfer transfer, boolean isBatch, String shard) {
        if (isBatch) {
            this.transferBatch(null, transfer, shard); 
        }else {
            this.transfer(null, transfer, shard);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void transfer(ClientSession clientSession, Transfer transfer) {
        this.transfer(clientSession, transfer, false, null);
    }

    private void transfer(ClientSession clientSession, Transfer transfer, String shard) {
        this.transfer(clientSession, transfer, false, shard);
    }

    private void transfer(ClientSession clientSession, Transfer t, boolean hasError, String shard) {
        StopWatch sw = new StopWatch();
        sw.start();
        int transferAmount = t.getToAccountId().size();
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        MongoCollection<TransferLog> transferLogCollection = database.getCollection(transferLogCollectionName, TransferLog.class);
        if (shard != null) {
            if ("hashed".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "HashedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "HashedShard", TransferLog.class);
            } else if ("ranged".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "RangedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);
            }
        }
        logger.info(Thread.currentThread().getName() + " Deduct $" + transferAmount + " from account " + t.getFromAccountId());
        // Account a = null;
        // Start - Tony update to findAndModify from find and update, find and update requires serial read isolation to lock find and update to avoid any changes in the middle
        // if (clientSession != null) {
        //     a = collection.find(clientSession, Filters.eq("_id", t.getFromAccountId())).first();
        // } else {
        //     a = collection.find(Filters.eq("_id", t.getFromAccountId())).first();
        // }
        // if (a == null || a.getBalance() < transferAmount) {
        //     logger.info("Account " + a.getId() + " have not enough balance, skip transfer");
        //     sw.stop();
        // } else {

        //     if (clientSession != null) {
        //         collection.updateOne(clientSession, Filters.eq("_id", t.getFromAccountId()),
        //                 Updates.inc("balance", -transferAmount));
        //     } else {
        //         collection.updateOne(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -transferAmount));
        //     }

        //     for (Integer id2 : t.getToAccountId()) {
        //         if (hasError && Math.random() > 0.8) {
        //             throw new RuntimeException("Unexpected error. Something went wrong");
        //         }
        //         if (clientSession != null) {
        //             collection.updateOne(clientSession, Filters.eq("_id", id2), Updates.inc("balance", 1));
        //         } else {
        //             collection.updateOne(Filters.eq("_id", id2), Updates.inc("balance", 1));
        //         }

        //         TransferLog log = new TransferLog(1, t.getFromAccountId(), id2);
        //         if (clientSession != null) {
        //             transferLogCollection.insertOne(clientSession, log);
        //         } else {
        //             transferLogCollection.insertOne(log);
        //         }
        //     }
        //     sw.stop();
            //logger.info((clientSession == null ? "" : ("clientSession: " + clientSession.getServerSession().getIdentifier().getBinary("id").asUuid() + " ")) + "Completed transfer $1x" + t.getToAccountId().size() + " from " + t.getFromAccountId() + " to " + Arrays.toString(t.getToAccountId().toArray()) + ". takes "
            //        + sw.getTotalTimeMillis() + "ms, TPS:" + (t.getToAccountId().size() + 1) / sw.getTotalTimeSeconds());
        // }
        // End - Tony update to findAndModify from find and update, find and update requires serial read isolation to lock find and update to avoid any changes in the middle
        Account _newBalance = null; 
        if (clientSession != null) {
            _newBalance =  collection.findOneAndUpdate(clientSession, Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -transferAmount));
        } else {
            _newBalance = collection.findOneAndUpdate(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -transferAmount));
        }
        if (_newBalance.getBalance() < 0) {
            logger.info("Account " + _newBalance.getId() + " have not enough balance, skip transfer");
            sw.stop();
            throw new RuntimeException("Account " + _newBalance.getId() + " have not enough balance, skip transfer");
        } else {

            TransferLog log = new TransferLog(1, t.getFromAccountId(), t.getToAccountId().get(0));
            if (clientSession != null) {
                transferLogCollection.insertOne(clientSession, log);
            } else {
                transferLogCollection.insertOne(log);
            }

            sw.stop();
        }
    }

    private void transferBatch(ClientSession clientSession, Transfer t, String shard) {

        StopWatch sw = new StopWatch();
        sw.start();
        MongoCollection<Account> collection = database.getCollection(collectionName, Account.class);
        MongoCollection<TransferLog> transferLogCollection = database.getCollection(transferLogCollectionName, TransferLog.class);
        if (shard != null) {
            if ("hashed".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "HashedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "HashedShard", TransferLog.class);
            } else if ("ranged".equalsIgnoreCase(shard)) {
                collection = database.getCollection(collectionName + "RangedShard", Account.class);
                transferLogCollection = database.getCollection(transferLogCollectionName + "RangedShard", TransferLog.class);
            }
        }
        List<UpdateOneModel<Account>> list = new ArrayList<>();
        List<InsertOneModel<TransferLog>> listTransferLog = new ArrayList<>();
        int transferAmount = t.getToAccountId().size();
        Account a = null;
        if (clientSession != null) {
            a = collection.find(clientSession, Filters.eq("_id", t.getFromAccountId())).first();
        } else {
            a = collection.find(Filters.eq("_id", t.getFromAccountId())).first();
        }
        if (a == null || a.getBalance() < transferAmount) {
            logger.info("Account " + (Objects.nonNull(a) ? a.getId() : "a = null") + " have not enough balance, skip transfer");
            sw.stop();
        } else {
            list.add(new UpdateOneModel<>(Filters.eq("_id", t.getFromAccountId()), Updates.inc("balance", -transferAmount)));
            for (Integer id2 : t.getToAccountId()) {
                list.add(new UpdateOneModel<>(Filters.eq("_id", id2), Updates.inc("balance", 1)));
                listTransferLog.add(new InsertOneModel<TransferLog>(new TransferLog(1, t.getFromAccountId(), id2)));
            }
            if (!list.isEmpty()) {
                if (clientSession != null) {
                    collection.bulkWrite(clientSession, list);
                    transferLogCollection.bulkWrite(clientSession, listTransferLog);
                } else {
                    collection.bulkWrite(list);
                    transferLogCollection.bulkWrite(listTransferLog);
                }
            }
            sw.stop();
            //logger.info((clientSession == null ? "" : ("clientSession: " + clientSession.getServerSession().getIdentifier().getBinary("id").asUuid() + " ")) + "Completed transfer, total " + (list.size() + listTransferLog.size()) + " operations takes "
            //        + sw.getTotalTimeMillis() + "ms, TPS:" + list.size() / sw.getTotalTimeSeconds());
        }
    }

    public void longTransaction(long waitTime, List<Transfer> transfers) throws InterruptedException {
        for (Transfer transfer : transfers) {
            UUID tranId = null;
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
                        tranId = clientSession.getServerSession().getIdentifier().getBinary("id").asUuid();
                        logger.info("Start Transaction: " + tranId);
                        transfer(clientSession, transfer);
                        logger.info("Start waiting for commit");
                        Thread.sleep(waitTime);
                        while (true) {
                            try {
                                clientSession.commitTransaction();
                                logger.info("Transaction committed: " + tranId);
                                break;
                            } catch (MongoException e) {
                                // can retry commit
                                if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                                    logger.info("UnknownTransactionCommitResult, retrying " + tranId + " commit operation ...");
                                    continue;
                                } else {
                                    logger.error("Exception during commit ...", e);
                                    throw e;
                                }
                            }
                        }
                    }
                    break;
                } catch (MongoException e) {
                    //logger.info("Transaction aborted. Caught exception during transaction.");
                    if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                        // e.printStackTrace();
                        logger.info("TransientTransactionError, aborting transaction " + tranId + " and retrying ...");
                        continue;
                    } else {
                        throw e;
                    }
                }
            }
        }

        // final ClientSession clientSession = client.startSession();
        // UUID tranId = clientSession.getServerSession().getIdentifier().getBinary("id").asUuid();
        // TransactionOptions txnOptions = TransactionOptions.builder()
        //         .readPreference(ReadPreference.primary())
        //         .readConcern(ReadConcern.MAJORITY)
        //         .writeConcern(WriteConcern.MAJORITY)
        //         .build();
        // TransactionBody<String> txnBody = new TransactionBody<String>() {
        //     public String execute() {
        //         logger.info("Start Transaction: "+tranId);
        //         transfer(clientSession, transfers);
        //         logger.info("Start waiting for commit");
        //         try {
        //             Thread.sleep(waitTime);
        //         } catch (InterruptedException e) {
        //             e.printStackTrace();
        //         }
        //         return "Transaction committed: "+tranId;
        //     }
        // };
        // try {
        //     logger.info(clientSession.withTransaction(txnBody, txnOptions));
        // } catch (RuntimeException e) {
        //     logger.error("Error during transfer", e);
        // } finally {
        //     clientSession.close();
        // }
    }
}
