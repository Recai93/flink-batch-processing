package reducer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.util.ArrayIterator;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import util.Constant;

import static org.mockito.Mockito.times;

public class ReducersTest {

    @Test
    public void itShouldReduceProductViews() {
        AllProductViewsReducer reducer = new AllProductViewsReducer();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);
        Tuple2[] tuples = {new Tuple2<>("491", "13"), new Tuple2<>("491", "15")};
        Iterable<Tuple2<String, String>> iterable = new ArrayIterator<>(tuples);

        reducer.reduce(iterable, collector);

        Mockito.verify(collector, times(1)).collect(new Tuple2<>("491", 2));
    }

    @Test
    public void itShouldReduceTopUsers() {
        TopUsersReducer reducer = new TopUsersReducer();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);
        Tuple3[] tuples = {new Tuple3<>("13", Constant.VIEW_PRODUCT_ACTION, 3),
                new Tuple3<>("13", Constant.CLICK_PRODUCT_ACTION, 1),
                new Tuple3<>("13", Constant.REMOVE_PRODUCT_ACTION, 1),
                new Tuple3<>("13", Constant.ADD_PRODUCT_ACTION, 2)};
        Iterable<Tuple3<String, String, Integer>> iterable = new ArrayIterator<>(tuples);

        reducer.reduce(iterable, collector);

        Mockito.verify(collector, times(1)).collect(new Tuple2<>("13", 7));
    }
}
