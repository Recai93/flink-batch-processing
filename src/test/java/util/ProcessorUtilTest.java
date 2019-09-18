package util;

import org.apache.flink.api.java.tuple.Tuple1;
import org.junit.Assert;
import org.junit.Test;

public class ProcessorUtilTest {

    @Test
    public void itShouldParseLine() {

        final Tuple1<String> stringTuple1 = Tuple1.of("iZmir|AnKara|isTANbuL");

        String[] array = ProcessorUtil.parseLine(stringTuple1);

        Assert.assertEquals(array[0], "izmir");
        Assert.assertEquals(array[1], "ankara");
        Assert.assertEquals(array[2], "istanbul");
    }
}
