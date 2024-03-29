package mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import util.ProcessorUtil;

import static util.Constant.*;

public class AllEventsMapper implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {

    @Override
    public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Integer>> out) {

        String[] tokens = ProcessorUtil.parseLine(value);

        if (tokens.length == FIELDS_NUMBER) {
            out.collect(new Tuple2<>(tokens[EVENT_NAME_INDEX], 1));
        }
    }
}