package reducer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

public class TopUsersReducerTest {

    @Test
    public void itShouldReduceTopUsers() {
        TopUsersReducer reducer = new TopUsersReducer();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);
        Tuple3[] tuples = {new Tuple3<>("13", "view", 3),
                new Tuple3<>("13", "click", 1),
                new Tuple3<>("13", "remove", 1),
                new Tuple3<>("13", "add", 2)};
        Iterable<Tuple3<String, String, Integer>> iterable = new ArrayIterator<>(tuples);

        reducer.reduce(iterable, collector);

        Mockito.verify(collector).collect(new Tuple2<>("13", 7));
    }
}
