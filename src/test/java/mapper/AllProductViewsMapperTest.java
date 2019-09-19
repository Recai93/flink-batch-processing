package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

public class AllProductViewsMapperTest {

    @Test
    public void itShouldMapAllProductViews() {
        AllProductViewsMapper mapper = new AllProductViewsMapper();
        Collector<Tuple2<String, String>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|491|view|13"), collector);

        Mockito.verify(collector).collect(new Tuple2<>("491", "13"));
    }
}
