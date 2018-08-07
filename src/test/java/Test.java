import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.GraqlSyntax;
import ai.grakn.util.SimpleURI;
import com.lolskillz.Main;

import java.util.List;
import java.util.concurrent.ExecutionException;

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

        try (Grakn.Session session = new Grakn(new SimpleURI(GRAKN_URI)).session(Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                String entType = "person14";
                tx.graql().define(label(entType).sub("entity")).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.graql().insert(var().isa(entType)).execute();
                List<ConceptMap> answers1 = tx.graql().match(var("x").isa(entType)).get().execute();
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

        try (Grakn.Session session = new Grakn(new SimpleURI(GRAKN_URI)).session(Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                List<ConceptMap> answers = tx.graql().match(var("x").isa("value")).get().execute();
                int i = 1;
                for (ConceptMap e: answers) {
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

        try (Grakn.Session session = new Grakn(new SimpleURI(GRAKN_URI)).session(Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                String entType = "person";
                tx.graql().define(label(entType).sub("entity")).execute();
                tx.graql().insert(var().isa(entType)).execute();
                tx.commit();
            }
        }
        try (Grakn.Session session = new Grakn(new SimpleURI(GRAKN_URI)).session(Keyspace.of(GRAKN_KEYSPACE))) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                long count = tx.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("entity").execute().get(0).number().longValue();
                System.out.println("entity count = " + count);
            }
        }
    }
}
