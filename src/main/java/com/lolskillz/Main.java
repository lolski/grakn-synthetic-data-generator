package com.lolskillz;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.ClientFactory;
import ai.grakn.client.Grakn;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.Match;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.GraqlSyntax;
import ai.grakn.util.SimpleURI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.query.aggregate.Aggregates.count;
import static com.lolskillz.ActionCount.verifyAndPrint;
import static com.lolskillz.ActionInsert.define;
import static com.lolskillz.ActionInsert.insertNameShuffled;
import static com.lolskillz.ActionInsert.insertPerson;
import static com.lolskillz.ActionInsert.relatePerson;

public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length == 0) {
            System.out.println("synthetic-data-generator usage:");
            System.out.println("- insert data: ./sdgen <host:port> <username> <password> <keyspace> insert <num-duplicates-per-entity> <num-entities> eg., ./sdgen localhost:48555 cassandra cassandra grakn insert 2 100");
            System.out.println("- count data: ./sdgen <host:port> <username> <password> <keyspace> count eg., ./sdgen localhost:48555 cassandra cassandra grakn count");
            System.exit(0);
        }

        //
        // Parameters
        //
        final String GRAKN_URI = args[0];
        final String USERNAME = args[1];
        final String PASSWORD = args[2];
        final String GRAKN_KEYSPACE = args[3];
        final String ACTION = args[4];
        final int DUPLICATE = Integer.parseInt(args[5]);
        final int NUM_ENTITIES = Integer.parseInt(args[6]);

        final ExecutorService executorService = Executors.newFixedThreadPool(8);

        //
        // Create a schema, then perform multi-threaded data insertion where each thread inserts exactly the same data
        //
        System.out.println("starting test with the following configuration: Grakn URI: " + GRAKN_URI + ", keyspace: " + GRAKN_KEYSPACE + ", thread: " + DUPLICATE + ", unique attribute: " + NUM_ENTITIES);
        Grakn.Session session = new ClientFactory(new SimpleURI(GRAKN_URI), USERNAME, PASSWORD).client().session(Keyspace.of(GRAKN_KEYSPACE));

        if (ACTION.equals("count")) {
            verifyAndPrint(session, NUM_ENTITIES);
        }
        else if (ACTION.equals("insert")) {
            System.out.println("defining schema...");
            CompletableFuture<Void> asyncAll = define(session)
                    .thenCompose(e -> insertNameShuffled(session, NUM_ENTITIES, DUPLICATE))
                    .thenCompose(e -> insertPerson(session, 0, NUM_ENTITIES, executorService))
                    .thenCompose(e -> relatePerson(session, NUM_ENTITIES));

            //
            // Cleanups: close the session and the executor service
            //
            asyncAll.whenComplete((r, ex) -> {
                session.close();
                try {
                    executorService.shutdown();
                    executorService.awaitTermination(10, TimeUnit.SECONDS);
                    if (ex == null) {
                        System.out.println("inserted " + NUM_ENTITIES + " entities.");
                        System.out.println("inserted " + NUM_ENTITIES * DUPLICATE + " attribute in total of which "
                                + NUM_ENTITIES + " is unique. if post-processing works correctly, grakn should have only " + NUM_ENTITIES + " attributes.");
                        System.out.println("inserted " + (NUM_ENTITIES-1) + " relationships.");
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
}

class ActionInsert {
    public static CompletableFuture<Void> define(Grakn.Session session) {
        return CompletableFuture.supplyAsync(() -> {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
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

    public static CompletableFuture<Void> insertNameShuffled(Grakn.Session session, int nameCount, int duplicatePerNameCount) {
        return CompletableFuture.supplyAsync(() -> {
            long startMeasure = System.currentTimeMillis();
            Random rng = new Random(1);

            List<Integer> names = new ArrayList<>();
            for (int i = 0; i < nameCount; ++i) {
                for (int j = 0; j < duplicatePerNameCount; ++j) {
                    names.add(i);
                }
            }
            Collections.shuffle(names, rng);

            for (int name: names) {
                boolean insertSucceeded = false;
                System.out.print("inserted a new name attribute with value '" + name + "'");
                while (!insertSucceeded) {
                    try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                        tx.graql().insert(var().isa("name").val(Integer.toString(name))).execute();
                        tx.commit();
                        insertSucceeded = true;
                    }
                    catch (RuntimeException e) {
                        System.out.print(".");
                    }
                }
                System.out.println();
            }

            System.out.println("elapsed = " + (System.currentTimeMillis() - startMeasure) + "ms");
            return null;
        });
    }

    public static CompletableFuture<Void> insertPerson(Grakn.Session session, int executionId, int n, ExecutorService executorService) {
        return CompletableFuture.supplyAsync(() -> {
            for (int i = 0; i < n; ++i) {
                if (n % 100 == 0) {
                    System.out.println("execution " + executionId + " is now inserting entities no. " + i + "...");
                }
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    tx.graql().insert(var().isa("person").has("name", Integer.toString(i))).execute();
                    tx.commit();
                }
            }
            return null;
        }, executorService);
    }

    public static CompletableFuture<Void> relatePerson(Grakn.Session session, int n) {
        System.out.println("relating " + n + " person(s) with a parent child relationship...");
        return CompletableFuture.supplyAsync(() -> {
            for (int i = 0; i < n - 1; ++i) {
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    String prntId = Integer.toString(i);
                    String chldId = Integer.toString(i + 1);
                    Match toBeLinked = tx.graql().match(
                            var("prnt").isa("person").has("name", prntId),
                            var("chld").isa("person").has("name", chldId));
                    toBeLinked.insert(var().isa("parentchild").rel("parent", "prnt").rel("child", "chld")).execute();
                    toBeLinked.get().execute().forEach(e -> System.out.println(e.get("prnt").id() + " name = " + prntId + " (prnt) --> (chld) " + e.get("chld").id() + " " + chldId));
                    tx.commit();
                }
            }
            return null;
        });
    }
}

class ActionCount {
    public static void verifyAndPrint(Grakn.Session session, int n) {
        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            for (int i = 0; i < n-1; ++i) { // n-1, because we're iterating up to the 2nd last person (as the last person doesn't have any child)
                String prntId = Integer.toString(i);
                String chldId = Integer.toString(i + 1);
                Match toBeLinked = tx.graql().match(
                        var("prnt").isa("person").has("name", prntId),
                        var("chld").isa("person").has("name", chldId),
                        var("prntchld").rel("parent", "prnt").rel("child", "chld")
                );
                List<ConceptMap> execute = toBeLinked.get().execute();
                if (execute.isEmpty()) {
                    System.err.println("NO RESULT FOR '" + toBeLinked.get().toString() + "'");
                }
                else {
                    execute.forEach(e -> System.out.println(e.get("prnt").id() + " name = " + prntId + " (prnt) --> (chld) " + e.get("chld").id() + " " + chldId + " via relationship '" + e.get("prntchld")));
                }
            }
        }

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            System.out.print("performing count using match - aggregate count...");
            long person = tx.graql().match(var("n").isa("person")).aggregate(count()).execute().get(0).number().longValue();
            long name = tx.graql().match(var("n").isa("name")).aggregate(count()).execute().get(0).number().longValue();
            System.out.println("person count = " + person + ", name = " + name);
        }

        try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
            System.out.print("performing count using compute count...");
            long person = tx.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("person").execute().get(0).number().longValue();
            long name = tx.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("name").execute().get(0).number().longValue();
            System.out.println("person count = " + person + ", name = " + name);
        }
    }
}