package userevents;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;
import util.Constants;
import util.Utils;

import java.util.List;

public class UserEventsProcessorTest {

    private static final String INPUT_FILE = "/Users/recaihuseyin/flink/flink-batch-processing/src/test/resources/input.csv";

    private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    private UserEventsProcessor processor = new UserEventsProcessor();

    @Test
    public void testGetProductViews() throws Exception {
        String expectedFirstProductId = "496";
        String expectedSecondProductId = "642";

        Integer expectedFirstProductViews = 3;
        Integer expectedSecondProductViews = 1;

        DataSet<Tuple2<String, Integer>> data = processor.getProductViews(Utils.readInput(INPUT_FILE, env));
        List<Tuple2<String, Integer>> tuples = data.collect();

        Assert.assertEquals(tuples.size(), 2);
        for (Tuple2<String, Integer> t : tuples) {
            if (t.f0.equals(expectedFirstProductId)) {
                Assert.assertEquals(expectedFirstProductViews, t.f1);
            } else if (t.f0.equals(expectedSecondProductId)) {
                Assert.assertEquals(expectedSecondProductViews, t.f1);
            }
        }
    }

    @Test
    public void testGetUserProductViews() throws Exception {
        String expectedProductId = "642";

        DataSet<Tuple1<String>> data = processor.getUserProductViews(Utils.readInput(INPUT_FILE, env));
        List<Tuple1<String>> tuples = data.collect();
        Assert.assertEquals(tuples.size(), 1);
        Assert.assertEquals(tuples.get(0).f0, expectedProductId);
    }

    @Test
    public void testGetUserEvents() throws Exception {
        Integer expectedProductViews = 2;

        DataSet<Tuple2<String, Integer>> data = processor.getUserEvents(Utils.readInput(INPUT_FILE, env));
        List<Tuple2<String, Integer>> tuples = data.collect();
        Assert.assertEquals(tuples.size(), 1);
        Assert.assertEquals(tuples.get(0).f0, Constants.VIEW_PRODUCT_ACTION);
        Assert.assertEquals(tuples.get(0).f1, expectedProductViews);
    }

    @Test
    public void testGetAllEvents() throws Exception {
        Integer expectedViewCount = 5;
        Integer expectedClickCount = 1;
        Integer expectedAddCount = 3;
        Integer expectedRemoveCount = 1;

        DataSet<Tuple2<String, Integer>> data = processor.getAllEvents(Utils.readInput(INPUT_FILE, env));
        List<Tuple2<String, Integer>> tuples = data.collect();

        Assert.assertEquals(tuples.size(), 4);
        for (Tuple2<String, Integer> t : tuples) {
            switch (t.f0) {
                case Constants.ADD_PRODUCT_ACTION:
                    Assert.assertEquals(expectedAddCount, t.f1);
                    break;
                case Constants.REMOVE_PRODUCT_ACTION:
                    Assert.assertEquals(expectedRemoveCount, t.f1);
                    break;
                case Constants.CLICK_PRODUCT_ACTION:
                    Assert.assertEquals(expectedClickCount, t.f1);
                    break;
                case Constants.VIEW_PRODUCT_ACTION:
                    Assert.assertEquals(expectedViewCount, t.f1);
                    break;
            }
        }
    }

    @Test
    public void testGetTopUser() throws Exception {
        String expectedTopUserId = "49";
        DataSet<Tuple1<String>> data = processor.getTopUsers(Utils.readInput(INPUT_FILE, env));
        Assert.assertEquals(expectedTopUserId, data.collect().get(0).f0);
    }
}
