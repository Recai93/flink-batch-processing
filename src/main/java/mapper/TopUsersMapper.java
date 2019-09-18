package mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import util.ProcessorUtil;

import static util.Constant.*;

public class TopUsersMapper implements FlatMapFunction<Tuple1<String>, Tuple3<String, String, Integer>> {

    @Override
    public void flatMap(Tuple1<String> value, Collector<Tuple3<String, String, Integer>> out) {

        String[] tokens = ProcessorUtil.parseLine(value);

        if (tokens.length == FIELDS_NUMBER) {
            out.collect(new Tuple3<>(tokens[USER_ID_INDEX], tokens[EVENT_NAME_INDEX], 1));
        }
    }
}