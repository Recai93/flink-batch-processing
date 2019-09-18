package userevents;

import mapper.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import reducer.AllProductViewsReducer;
import reducer.TopUsersReducer;

import static util.Constants.TOP_USERS_NUMBER;

public class UserEventsProcessor {

    public DataSet<Tuple2<String, Integer>> getAllEvents(DataSet<Tuple1<String>> lines) {
        return lines
                .flatMap(new AllEventsMapper())
                .groupBy(0)
                .sum(1);
    }

    public DataSet<Tuple2<String, Integer>> getProductViews(DataSet<Tuple1<String>> lines) {
        return lines
                .flatMap(new AllProductViewsMapper())
                .groupBy(0)
                .reduceGroup(new AllProductViewsReducer());
    }

    public DataSet<Tuple2<String, Integer>> getUserEvents(DataSet<Tuple1<String>> lines) {
        return lines
                .flatMap(new UserEventsMapper())
                .groupBy(0)
                .sum(1);
    }

    public DataSet<Tuple1<String>> getUserProductViews(DataSet<Tuple1<String>> lines) {
        return lines
                .flatMap(new UserProductViewsMapper())
                .distinct();
    }

    public DataSet<Tuple1<String>> getTopUsers(DataSet<Tuple1<String>> lines) {
        return lines
                .flatMap(new TopUsersMapper())
                .groupBy(0)
                .reduceGroup(new TopUsersReducer())
                .sortPartition(1, Order.DESCENDING)
                .first(TOP_USERS_NUMBER)
                .flatMap(new TopUsersIdMapper());
    }
}