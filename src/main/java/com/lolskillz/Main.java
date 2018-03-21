package com.lolskillz;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.Match;
import ai.grakn.graql.admin.Answer;
import ai.grakn.kgms.remote.RemoteKGMS;
import ai.grakn.util.SimpleURI;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.*;

public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //
        // Parameters
        //
        final String GRAKN_URI = System.getenv("GRAKN_URI") != null ? System.getenv("GRAKN_URI") : "localhost:48555";
        final String GRAKN_KEYSPACE = System.getenv("GRAKN_KEYSPACE") != null ? System.getenv("GRAKN_KEYSPACE") : "grakn";
        final int DUPLICATE = System.getenv("DUPLICATE") != null ? Integer.parseInt(System.getenv("DUPLICATE")) : 1;
        final int NUM_ENTITIES = System.getenv("NUM_ENTITIES") != null ? Integer.parseInt(System.getenv("NUM_ENTITIES")) : 200;
        final String ACTION = System.getenv("ACTION") != null ? System.getenv("ACTION") : "count";

        final ExecutorService executorService = Executors.newFixedThreadPool(DUPLICATE);

        //
        // Create a schema, then perform multi-threaded data insertion where each thread inserts exactly the same data
        //
        System.out.println("starting test with the following configuration: Grakn URI: " + GRAKN_URI + ", keyspace: " + GRAKN_KEYSPACE + ", thread: " + DUPLICATE + ", unique attribute: " + NUM_ENTITIES);
        GraknSession session = RemoteKGMS.session(new SimpleURI(GRAKN_URI), Paths.get("./trustedCert.crt"), Keyspace.of(GRAKN_KEYSPACE), "cassandra", "cassandra");

//        GraknSession session = RemoteGrakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE));

        if (ACTION.equals("count")) {
            verifyAndPrint(session, NUM_ENTITIES);
        }
        else if (ACTION.equals("insert")) {
            System.out.println("defining schema...");
            CompletableFuture<Void> asyncAll = define(session)
                    .thenCompose(e -> insertName(session, NUM_ENTITIES, DUPLICATE, executorService))
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

    private static CompletableFuture<Void> insertName(GraknSession session, int nameCount, int duplicatePerNameCount, ExecutorService executorService) {
        // insert name with value 'name'
        Consumer<Integer> insert = name -> {
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                tx.graql().insert(var().isa("name").val(Integer.toString(name))).execute();
                tx.commit();
            }
        };

        // insert name with value 'name', along with 'duplicatePerNameCount' duplicates to test post-processing
        Function<Integer, CompletableFuture<Void>> insertWithDuplicateInParallel = name -> {
            Stream<CompletableFuture<Void>> insertWithDuplicate = IntStream.range(0, duplicatePerNameCount)
                    .mapToObj(d -> CompletableFuture.<Void>supplyAsync(() -> {
                        insert.accept(name);
                        System.out.println("inserted copy no " + d + " for the name attribute with value '" + name + "'");
                        return null;
                    }, executorService));

            final List<CompletableFuture<Void>> collect = insertWithDuplicate.collect(Collectors.toList());
            return CompletableFuture.allOf(collect.toArray(new CompletableFuture[duplicatePerNameCount]));
        };

        // insert a 'nameCount' number of name attribute, with'duplicate' number of duplicate
        Function<Integer, CompletableFuture<Void>> insertAllInParallel = count -> {
            Stream<CompletableFuture<Void>> insertWithDuplicate = IntStream.range(0, count).mapToObj(insertWithDuplicateInParallel::apply);
            final List<CompletableFuture<Void>> collect = insertWithDuplicate.collect(Collectors.toList());
            return CompletableFuture.allOf(collect.toArray(new CompletableFuture[nameCount]));
        };

        return insertAllInParallel.apply(nameCount);
    }

    private static CompletableFuture<Void> insertPerson(GraknSession session, int executionId, int n, ExecutorService executorService) {
        return CompletableFuture.supplyAsync(() -> {
            for (int i = 0; i < n; ++i) {
                if (n % 100 == 0) {
                    System.out.println("execution " + executionId + " is now inserting entities no. " + i + "...");
                }
                try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                    tx.graql().insert(var().isa("person").has("name", Integer.toString(i))).execute();
                    tx.commit();
                }
            }
            return null;
        }, executorService);
    }

    private static CompletableFuture<Void> relatePerson(GraknSession session, int n) {
        System.out.println("relating " + n + " person(s) with a parent child relationship...");
        return CompletableFuture.supplyAsync(() -> {
            for (int i = 0; i < n - 1; ++i) {
                try (GraknTx tx = session.open(GraknTxType.WRITE)) {
                    String prntId = Integer.toString(i);
                    String chldId = Integer.toString(i + 1);
                    Match toBeLinked = tx.graql().match(
                            var("prnt").isa("person").has("name", prntId),
                            var("chld").isa("person").has("name", chldId));
                    toBeLinked.insert(var().isa("parentchild").rel("parent", "prnt").rel("child", "chld")).execute();
                    toBeLinked.get().execute().forEach(e -> System.out.println(e.get("prnt").getId() + " name = " + prntId + " (prnt) --> (chld) " + e.get("chld").getId() + " " + chldId));
                    tx.commit();
                }
            }
            return null;
        });
    }

    private static void verifyAndPrint(GraknSession session, int n) {
        try (GraknTx tx = session.open(GraknTxType.WRITE)) {
            for (int i = 0; i < n-1; ++i) { // n-1, because we're iterating up to the 2nd last person (as the last person doesn't have any child)
                String prntId = Integer.toString(i);
                String chldId = Integer.toString(i + 1);
                Match toBeLinked = tx.graql().match(
                        var("prnt").isa("person").has("name", prntId),
                        var("chld").isa("person").has("name", chldId));
                List<Answer> execute = toBeLinked.get().execute();
                if (execute.isEmpty()) {
                    System.err.println("NO RESULT FOR '" + toBeLinked.get().toString() + "'");
                }
                else {
                    execute.forEach(e -> System.out.println(e.get("prnt").getId() + " name = " + prntId + " (prnt) --> (chld) " + e.get("chld").getId() + " " + chldId));
                }
            }
        }

        try (GraknTx tx = session.open(GraknTxType.READ)) {
//            long person = tx.graql().compute().count().in("person").execute();
//            long name = tx.graql().compute().count().in("name").execute();
            long person = tx.graql().match(var("n").isa("person")).aggregate(count()).execute();
            long name = tx.graql().match(var("n").isa("name")).aggregate(count()).execute();
            System.out.println("person count = " + person + ", name = " + name);
        }
    }
}
