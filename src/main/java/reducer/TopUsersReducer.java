package reducer;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

import static util.Constants.ALL_ACTIONS_ARRAY;

public class TopUsersReducer implements GroupReduceFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>> {

    @Override
    public void reduce(Iterable<Tuple3<String, String, Integer>> in, Collector<Tuple2<String, Integer>> out) {

        Integer count = 0;
        Set<String> actions = new HashSet<>();
        String id = null;

        for (Tuple3<String, String, Integer> t : in) {
            id = t.f0;
            actions.add(t.f1);
            count += t.f2;
        }

        if (actions.containsAll(ALL_ACTIONS_ARRAY)) {
            out.collect(new Tuple2<>(id, count));
        }
    }
}