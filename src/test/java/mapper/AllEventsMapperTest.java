package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

public class AllEventsMapperTest {

    @Test
    public void itShouldMapAllEvents() {
        AllEventsMapper mapper = new AllEventsMapper();
        Collector<Tuple2<String, Integer>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|496|click|13"), collector);

        Mockito.verify(collector).collect(new Tuple2<>("click", 1));
    }
}
