package com.lolskillz;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ai.grakn.graql.Graql.*;

public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        final String GRAKN_URI = "localhost:4567";
        final String GRAKN_KEYSPACE = "grakn";
        final int N_THREAD = 15;
        final int N_ATTRIBUTE = 1000;
        final ExecutorService executorService = Executors.newFixedThreadPool(N_THREAD);

        System.out.println("starting test. defining schema...");
        GraknSession session = Grakn.session(GRAKN_URI, GRAKN_KEYSPACE);

        define(session);
        List<CompletableFuture<Void>> asyncAll = insertAsync(executorService, session, N_ATTRIBUTE, N_THREAD);

        CompletableFuture<Void> waitAll = CompletableFuture.allOf(asyncAll.toArray(new CompletableFuture[asyncAll.size()]));
        waitAll.whenComplete((r, ex) -> {
            System.out.println("inserting " + N_ATTRIBUTE * N_THREAD + " attribute in total of which "
                    + N_ATTRIBUTE + " is unique. if post-processing works correctly, grakn should have only " + N_ATTRIBUTE + " attributes.");
            try {
                executorService.shutdown();
                executorService.awaitTermination(300, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            session.close();
        }).get();
    }

    private static List<CompletableFuture<Void>> insertAsync(ExecutorService executorService, GraknSession session, int N_ATTRIBUTE, int N_THREAD) {
        List<CompletableFuture<Void>> all = new LinkedList<>();

        for (int i = 0; i < N_THREAD; ++i) {
            final int executionId = i;
            System.out.println("execution " + i + "starting...");

            CompletableFuture<Void> future = CompletableFuture.<Void>supplyAsync(() -> {
                insert(session, executionId, N_ATTRIBUTE);
                return null;
            }, executorService).exceptionally(ex2 -> {
                ex2.printStackTrace(System.err);
                return null;
            });

            all.add(future);
        }
        return all;
    }

    private static void define(GraknSession session) {
        try (GraknTx tx = session.open(GraknTxType.WRITE)) {
            tx.graql().define(label("value").sub("attribute").datatype(AttributeType.DataType.STRING)).execute();
            tx.commit();
        }
    }

    private static void insert(GraknSession session, int executionId, int n) {
        for (int i = 0; i < n; ++i) {
            if (n % 100 == 0) {
                System.out.println("execution " + executionId + " is now inserting attribute(s) no. " + i + "...");
            }
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                tx.graql().insert(var().isa("value").val(Integer.toString(i))).execute();
                tx.commit();
            }
        }
    }
}
