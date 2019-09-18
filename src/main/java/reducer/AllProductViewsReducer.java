package reducer;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class AllProductViewsReducer implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, Integer>> {

    @Override
    public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple2<String, Integer>> out) {

        Set<String> userIds = new HashSet<>();
        String productId = null;

        for (Tuple2<String, String> t : in) {
            productId = t.f0;
            userIds.add(t.f1);
        }

        out.collect(new Tuple2<>(productId, userIds.size()));
    }
}