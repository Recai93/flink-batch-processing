package mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TopUsersIdMapper implements FlatMapFunction<Tuple2<String, Integer>, Tuple1<String>> {

    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple1<String>> out) {
        out.collect(new Tuple1<>(value.f0));
    }
}