package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

public class UserProductViewsMapperTest {

    @Test
    public void itShouldMapUserProductViews() {
        UserProductViewsMapper mapper = new UserProductViewsMapper();
        Collector<Tuple1<String>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|554|view|47"), collector);

        Mockito.verify(collector).collect(new Tuple1<>("554"));
    }
}
