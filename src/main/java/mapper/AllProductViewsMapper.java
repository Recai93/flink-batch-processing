package mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import util.Utils;

import static util.Constants.*;

public class AllProductViewsMapper implements FlatMapFunction<Tuple1<String>, Tuple2<String, String>> {

    @Override
    public void flatMap(Tuple1<String> value, Collector<Tuple2<String, String>> out) {

        String[] tokens = Utils.parseLine(value);

        if (tokens.length == FIELDS_NUMBER && tokens[EVENT_NAME_INDEX].equals(VIEW_PRODUCT_ACTION)) {
            out.collect(new Tuple2<>(tokens[PRODUCT_ID_INDEX], tokens[USER_ID_INDEX]));
        }
    }
}