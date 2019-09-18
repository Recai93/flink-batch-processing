package util;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;

import static util.Constant.DELIMITER_REGEX;

public class ProcessorUtil {

    private ProcessorUtil() {
    }

    public static String[] parseLine(Tuple1<String> value) {
        return value.getField(0).toString().trim().toLowerCase().split(DELIMITER_REGEX);
    }

    public static DataSource<Tuple1<String>> readInput(String filePath, ExecutionEnvironment env) {
        return env.readCsvFile(filePath).ignoreFirstLine().types(String.class);
    }
}
