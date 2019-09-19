package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import util.Constant;

public class UserEventsMapperTest {

    @Test
    public void itShouldMapUserEvents() {
        UserEventsMapper mapper = new UserEventsMapper();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|455|view|47"), collector);

        Mockito.verify(collector).collect(new Tuple2<>(Constant.VIEW_PRODUCT_ACTION, 1));
    }
}
