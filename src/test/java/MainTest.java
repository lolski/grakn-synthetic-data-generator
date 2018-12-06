import com.lolskillz.Main;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class MainTest {
    @Test
    public void runMain() throws ExecutionException, InterruptedException {
        Main.main(new String[] { "localhost:48555", "cassandra", "cassandra", "grakn_pp8", "insert", "2", "10" });
    }
}
