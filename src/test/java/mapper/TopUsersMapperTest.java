package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import util.Constant;

public class TopUsersMapperTest {

    @Test
    public void itShouldMapTopUsers() {
        TopUsersMapper mapper = new TopUsersMapper();
        Collector<Tuple3<String, String, Integer>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple1<>("1535816823|495|view|15"), collector);

        Mockito.verify(collector).collect(new Tuple3<>("15", Constant.VIEW_PRODUCT_ACTION, 1));
    }
}
