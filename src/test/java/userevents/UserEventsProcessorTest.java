package userevents;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;
import util.Constant;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class UserEventsProcessorTest {

    private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    private UserEventsProcessor processor = new UserEventsProcessor();

    @Test
    public void itShouldGetProductViews() throws Exception {
        DataSource<Tuple1<String>> input = env
                .fromElements(new Tuple1<>("1535816823|496|click|13"),
                        new Tuple1<>("1536392928|496|add|69"),
                        new Tuple1<>("1536272308|642|view|47"),
                        new Tuple1<>("1536272308|642|view|47"),
                        new Tuple1<>("1536392928|642|view|69"));

        List<Tuple2<String, Integer>> tuples = processor.getProductViews(input).collect();

        assertEquals(1, tuples.size());
        assertEquals("642", tuples.get(0).f0);
        assertEquals(2, tuples.get(0).f1.intValue());
    }

    @Test
    public void itShouldGetUserProductViews() throws Exception {
        DataSource<Tuple1<String>> input = env
                .fromElements(new Tuple1<>("1535816823|496|view|41"),
                        new Tuple1<>("1536392928|496|add|69"),
                        new Tuple1<>("1536272308|642|view|47"),
                        new Tuple1<>("1536757406|164|remove|49"));

        List<Tuple1<String>> tuples = processor.getUserProductViews(input).collect();

        assertEquals(1, tuples.size());
        assertEquals("642", tuples.get(0).f0);
    }

    @Test
    public void itShouldGetUserEvents() throws Exception {
        DataSource<Tuple1<String>> input = env
                .fromElements(new Tuple1<>("1535816823|496|view|47"),
                        new Tuple1<>("1536392928|496|add|69"),
                        new Tuple1<>("1536272308|642|view|47"),
                        new Tuple1<>("1536757406|164|remove|49"));

        List<Tuple2<String, Integer>> tuples = processor.getUserEvents(input).collect();

        assertEquals(1, tuples.size());
        assertEquals(Constant.VIEW_PRODUCT_ACTION, tuples.get(0).f0);
        assertEquals(2, tuples.get(0).f1.intValue());
    }

    @Test
    public void itShouldGetAllEvents() throws Exception {
        DataSource<Tuple1<String>> input = env
                .fromElements(new Tuple1<>("1535816823|496|view|13"),
                        new Tuple1<>("1536392928|496|add|69"),
                        new Tuple1<>("1536272308|642|view|47"),
                        new Tuple1<>("1536757406|164|remove|49"),
                        new Tuple1<>("1536757406|164|add|49"),
                        new Tuple1<>("1536757406|164|click|49"));

        List<Tuple2<String, Integer>> tuples = processor.getAllEvents(input).collect();

        assertEquals(4, tuples.size());
        for (Tuple2<String, Integer> t : tuples) {
            if (t.f0.equals(Constant.ADD_PRODUCT_ACTION)) {
                assertEquals(2, t.f1.intValue());
            } else if (t.f0.equals(Constant.REMOVE_PRODUCT_ACTION)) {
                assertEquals(1, t.f1.intValue());
            } else if (t.f0.equals(Constant.CLICK_PRODUCT_ACTION)) {
                assertEquals(1, t.f1.intValue());
            } else if (t.f0.equals(Constant.VIEW_PRODUCT_ACTION)) {
                assertEquals(2, t.f1.intValue());
            }
        }
    }

    @Test
    public void itShouldGetTopUser() throws Exception {
        DataSource<Tuple1<String>> input = env
                .fromElements(new Tuple1<>("1535816823|496|view|13"),
                        new Tuple1<>("1536392928|496|add|69"),
                        new Tuple1<>("1536272308|642|view|47"),
                        new Tuple1<>("1536757406|164|remove|49"),
                        new Tuple1<>("1536757406|164|add|49"),
                        new Tuple1<>("1536757406|164|click|49"),
                        new Tuple1<>("1536392928|642|view|49"));

        List<Tuple1<String>> tuples = processor.getTopUsers(input).collect();

        assertEquals(1, tuples.size());
        assertEquals("49", tuples.get(0).f0);
    }
}
