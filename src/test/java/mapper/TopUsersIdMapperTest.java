package mapper;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

public class TopUsersIdMapperTest {

    @Test
    public void itShouldMapTopUsersId() {
        TopUsersIdMapper mapper = new TopUsersIdMapper();
        Collector<Tuple1<String>> collector = Mockito.mock(Collector.class);

        mapper.flatMap(new Tuple2<>("13", 4), collector);

        Mockito.verify(collector).collect(new Tuple1<>("13"));
    }
}
