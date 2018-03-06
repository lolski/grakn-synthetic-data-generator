package com.lolskillz;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.grakn.graql.Graql.*;

public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //
        // Parameters
        //
        final String GRAKN_URI = System.getenv("GRAKN_URI") != null ? System.getenv("GRAKN_URI") : "localhost:48555";
        final String GRAKN_KEYSPACE = System.getenv("GRAKN_KEYSPACE") != null ? System.getenv("GRAKN_KEYSPACE") : "grakn";
        final int N_THREAD = System.getenv("N_THREAD") != null ? Integer.parseInt(System.getenv("N_THREAD")) : 4;
        final int N_ATTRIBUTE = System.getenv("N_ATTRIBUTE") != null ? Integer.parseInt(System.getenv("N_ATTRIBUTE")) : 200;

        final ExecutorService executorService = Executors.newFixedThreadPool(N_THREAD);

        //
        // Create a schema, then perform multi-threaded data insertion where each thread inserts exactly the same data
        //
        System.out.println("starting test with the following configuration: Grakn URI: " + GRAKN_URI + ", keyspace: " + GRAKN_KEYSPACE + ", thread: " + N_THREAD + ", unique attribute: " + N_ATTRIBUTE);
        System.out.println("defining schema...");
        GraknSession session = RemoteGrakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE));

        CompletableFuture<Void> asyncAll = define(session)
                .thenCompose(e -> insertMultithreadedExecution(executorService, session, N_ATTRIBUTE, N_THREAD));

        //
        // Cleanups: close the session and the executor service
        //
        asyncAll.whenComplete((r, ex) -> {
            System.out.println("inserted " + N_ATTRIBUTE * N_THREAD + " attribute in total of which "
                    + N_ATTRIBUTE + " is unique. if post-processing works correctly, grakn should have only " + N_ATTRIBUTE + " attributes.");
            session.close();
            try {
                executorService.shutdown();
                executorService.awaitTermination(10, TimeUnit.SECONDS);
                System.out.println("test halted");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).get();
    }

    private static CompletableFuture<Void> insertMultithreadedExecution(ExecutorService executorService, GraknSession session, int numOfAttributes, int numOfExecutions) {
        List<CompletableFuture<Void>> executions = new LinkedList<>();

        for (int i = 0; i < numOfExecutions; ++i) {
            final int executionId = i;
            System.out.println("execution " + i + " starting...");

            CompletableFuture<Void> insertExecution = CompletableFuture.<Void>supplyAsync(() -> {
                insert(session, executionId, numOfAttributes);
                return null;
            }, executorService).exceptionally(ex2 -> {
                ex2.printStackTrace(System.err);
                return null;
            });

            executions.add(insertExecution);
        }

        return CompletableFuture.allOf(executions.toArray(new CompletableFuture[executions.size()]));
    }

    private static CompletableFuture<Void> define(GraknSession session) {
        return CompletableFuture.<Void>supplyAsync(() -> {
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                tx.graql().define(label("value").sub("attribute").datatype(AttributeType.DataType.STRING)).execute();
                tx.commit();
            }
            return null;
        });
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
