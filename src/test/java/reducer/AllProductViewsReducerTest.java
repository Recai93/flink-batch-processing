package reducer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

public class AllProductViewsReducerTest {

    @Test
    public void itShouldReduceProductViews() {
        AllProductViewsReducer reducer = new AllProductViewsReducer();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);
        Tuple2[] tuples = {new Tuple2<>("491", "13"), new Tuple2<>("491", "15")};
        Iterable<Tuple2<String, String>> iterable = new ArrayIterator<>(tuples);

        reducer.reduce(iterable, collector);

        Mockito.verify(collector).collect(new Tuple2<>("491", 2));
    }
}
