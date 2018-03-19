package com.lolskillz;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.Match;
import ai.grakn.kgms.remote.RemoteKGMS;
import ai.grakn.util.SimpleURI;

import java.nio.file.Paths;
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
        final String ACTION = System.getenv("ACTION") != null ? System.getenv("ACTION") : "count";

        final ExecutorService executorService = Executors.newFixedThreadPool(N_THREAD);

        //
        // Create a schema, then perform multi-threaded data insertion where each thread inserts exactly the same data
        //
        System.out.println("starting test with the following configuration: Grakn URI: " + GRAKN_URI + ", keyspace: " + GRAKN_KEYSPACE + ", thread: " + N_THREAD + ", unique attribute: " + N_ATTRIBUTE);
        GraknSession session = RemoteKGMS.session(new SimpleURI(GRAKN_URI), Paths.get("./trustedCert.crt"), Keyspace.of(GRAKN_KEYSPACE), "cassandra", "cassandra");

        if (ACTION.equals("count")) {
            verifyAndPrint(session, N_ATTRIBUTE);
        }
        else if (ACTION.equals("insert")) {
            System.out.println("defining schema...");
            CompletableFuture<Void> asyncAll = define(session)
                    .thenCompose(e -> insertMultithreadedExecution(executorService, session, N_ATTRIBUTE, N_THREAD))
                    .thenCompose(e -> relate(session, N_ATTRIBUTE));

            //
            // Cleanups: close the session and the executor service
            //
            asyncAll.whenComplete((r, ex) -> {
                session.close();
                try {
                    executorService.shutdown();
                    executorService.awaitTermination(10, TimeUnit.SECONDS);
                    if (ex == null) {
                        System.out.println("inserted " + N_ATTRIBUTE * N_THREAD + " attribute in total of which "
                                + N_ATTRIBUTE + " is unique. if post-processing works correctly, grakn should have only " + N_ATTRIBUTE + " attributes.");
                        System.out.println("test finished successfully!");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }).get();
        }
        else {
            System.err.println("ACTION must be 'count' or 'insert'");
        }
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
        return CompletableFuture.supplyAsync(() -> {
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                tx.graql().define(
                        label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                        label("parent").sub("role"),
                        label("child").sub("role"),
                        label("person").sub("entity").has("name").plays("parent").plays("child"),
                        label("parentchild").sub("relationship").relates("parent").relates("child")
                    ).execute();
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
                tx.graql().insert(var().isa("person").has("name", Integer.toString(i))).execute();
                tx.commit();
            }
        }
    }

    private static CompletableFuture<Void> relate(GraknSession session, int n) {
        System.out.println("relating " + n + " person(s) with a parent child relationship...");
        return CompletableFuture.supplyAsync(() -> {
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                for (int i = 0; i < n - 1; ++i) {
                    Match toBeLinked = tx.graql()
                        .match(
                            var("prnt").isa("person").has("name", Integer.toString(i)),
                            var("chld").isa("person").has("name", Integer.toString(i + 1)));

                    toBeLinked.insert(var().isa("parentchild").rel("parent", "prnt").rel("child", "chld")).execute();
                }
                tx.commit();
            }
            return null;
        });
    }

    private static void verifyAndPrint(GraknSession session, int n) {
        try (GraknTx tx = session.open(GraknTxType.WRITE)) {
            for (int i = 0; i < n; ++i) {
                String prntId = Integer.toString(i);
                String chldId = Integer.toString(i + 1);
                Match toBeLinked = tx.graql()
                        .match(
                                var("prnt").isa("person").has("name", prntId),
                                var("chld").isa("person").has("name", chldId));

                toBeLinked.get().execute().forEach(e -> System.out.println(e.get("prnt").getId() + " name = " + prntId + " (prnt) --> (chld) " + e.get("chld").getId() + " " + chldId));
            }
        }
    }
}
