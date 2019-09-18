package mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import util.Utils;

import static util.Constants.*;

public class UserProductViewsMapper implements FlatMapFunction<Tuple1<String>, Tuple1<String>> {

    @Override
    public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) {

        String[] tokens = Utils.parseLine(value);

        if (tokens.length == FIELDS_NUMBER && tokens[USER_ID_INDEX].equals(PRODUCT_VIEWER_USER_ID)
                && tokens[EVENT_NAME_INDEX].equals(VIEW_PRODUCT_ACTION)) {
            out.collect(new Tuple1<>(tokens[PRODUCT_ID_INDEX]));
        }
    }
}