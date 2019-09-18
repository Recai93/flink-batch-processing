package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import util.Constant;

import static org.mockito.Mockito.times;

public class MappersTest {

    @Test
    public void itShouldMapAllEvents() {
        AllEventsMapper mapper = new AllEventsMapper();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|496|click|13"), collector);

        Mockito.verify(collector, times(1)).collect(new Tuple2<>(Constant.CLICK_PRODUCT_ACTION, 1));
    }

    @Test
    public void itShouldMapAllProductViews() {
        AllProductViewsMapper mapper = new AllProductViewsMapper();
        Collector<Tuple2<String, String>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|491|view|13"), collector);

        Mockito.verify(collector, times(1)).collect(new Tuple2<>("491", "13"));
    }

    @Test
    public void itShouldMapTopUsers() {
        TopUsersMapper mapper = new TopUsersMapper();
        Collector<Tuple3<String, String, Integer>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|495|view|15"), collector);

        Mockito.verify(collector, times(1)).collect(new Tuple3<>("15", Constant.VIEW_PRODUCT_ACTION, 1));
    }

    @Test
    public void itShouldMapTopUsersId() {
        TopUsersIdMapper mapper = new TopUsersIdMapper();
        Collector<Tuple1<String>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple2<>("13", 4), collector);

        Mockito.verify(collector, times(1)).collect(new Tuple1<>("13"));
    }

    @Test
    public void itShouldMapUserEvents() {
        UserEventsMapper mapper = new UserEventsMapper();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|455|view|47"), collector);

        Mockito.verify(collector, times(1)).collect(new Tuple2<>(Constant.VIEW_PRODUCT_ACTION, 1));
    }

    @Test
    public void itShouldMapUserProductViews() {
        UserProductViewsMapper mapper = new UserProductViewsMapper();
        Collector<Tuple1<String>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|554|view|47"), collector);

        Mockito.verify(collector, times(1)).collect(new Tuple1<>("554"));
    }
}
