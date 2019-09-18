package userevents;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import util.ProcessorUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

import static util.Constant.*;

public class MainApp {

    private static final Logger logger = Logger.getLogger(MainApp.class.getName());

    public static void main(String[] args) {
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setGlobalJobParameters(params);

            DataSet<Tuple1<String>> lines;
            if (params.has(INPUT_FILE_PARAMETER)) {

                lines = ProcessorUtil.readInput(params.get(INPUT_FILE_PARAMETER), env);

                String outputDir = "";
                if (params.has(OUTPUT_DIR_PARAMETER)) {
                    outputDir = params.get(OUTPUT_DIR_PARAMETER);
                } else {
                    logger.info("output-dir parameter not found, will create output files in the current directory.");
                }

                UserEventsProcessor processor = new UserEventsProcessor();

                processor.getAllEvents(lines).writeAsCsv(outputDir + ALL_EVENTS_OUTPUT, ROW_DELIMITER,
                        DELIMITER, FileSystem.WriteMode.OVERWRITE);
                processor.getProductViews(lines).writeAsCsv(outputDir + PRODUCT_VIEWS_OUTPUT, ROW_DELIMITER,
                        DELIMITER, FileSystem.WriteMode.OVERWRITE);
                processor.getUserEvents(lines).writeAsCsv(outputDir + USER_EVENTS_OUTPUT, ROW_DELIMITER,
                        DELIMITER, FileSystem.WriteMode.OVERWRITE);
                processor.getUserProductViews(lines).writeAsCsv(outputDir + USER_PRODUCT_VIEWS_OUTPUT, ROW_DELIMITER,
                        DELIMITER, FileSystem.WriteMode.OVERWRITE);
                processor.getTopUsers(lines).writeAsCsv(outputDir + TOP_USERS_OUTPUT, ROW_DELIMITER,
                        DELIMITER, FileSystem.WriteMode.OVERWRITE);

                env.execute("User Events Batch Processing");

            } else {
                logger.info("input-file parameter not found, exiting program!");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "error while executing flink job!", e);
        }
    }
}