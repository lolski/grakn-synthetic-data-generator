import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.remote.RemoteGrakn;
import ai.grakn.util.SimpleURI;

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

        try (GraknSession session = RemoteGrakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE))) {
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
//                tx.graql().define(label("person").sub("entity")).execute();
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

        try (GraknSession session = RemoteGrakn.session(new SimpleURI(GRAKN_URI), Keyspace.of(GRAKN_KEYSPACE))) {
            try (GraknTx tx = session.open(GraknTxType.WRITE)) {
//                tx.graql().match(var().isa("person")).get().execute();
            }
        }
    }
}
