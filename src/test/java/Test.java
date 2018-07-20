import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.graql.admin.Answer;
import ai.grakn.util.GraqlSyntax;
import ai.grakn.util.SimpleURI;

import java.util.List;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;

public class Test {
    @org.junit.Test
    public void testA() {
        //
        // Parameters
        //
        final String GRAKN_URI = "localhost:48555";
        final String GRAKN_KEYSPACE = "grakn";

        try (Grakn.Session session = Grakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                String entType = "person14";
                tx.graql().define(label(entType).sub("entity")).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.graql().insert(var().isa(entType)).execute();
                List<Answer> answers1 = tx.graql().match(var("x").isa(entType)).get().execute();
                System.out.println();
                tx.commit();
            }
        }
    }

    @org.junit.Test
    public void testB() {
        //
        // Parameters
        //
        final String GRAKN_URI = "localhost:48555";
        final String GRAKN_KEYSPACE = "grakn";

        try (Grakn.Session session = Grakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                List<Answer> answers = tx.graql().match(var("x").isa("value")).get().execute();
                int i = 1;
                for (Answer e: answers) {
                    System.out.println(i  + ". " + e.get("x").id() + " -- " + e.get("x").isAttribute());
                    ++i;
                }
            }
        }
    }

    @org.junit.Test
    public void testComputeCount() {
        final String GRAKN_URI = "localhost:48555";
        final String GRAKN_KEYSPACE = "grakn2";

        try (Grakn.Session session = Grakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                String entType = "person";
                tx.graql().define(label(entType).sub("entity")).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.commit();
            }
        }
        try (Grakn.Session session = Grakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                long count = tx.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("entity").execute().getNumber().get().longValue();
                System.out.println("entity count = " + count);
            }
        }
    }
}
