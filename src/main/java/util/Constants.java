package util;

import java.util.Arrays;
import java.util.List;

public class Constants {

    public static final String ALL_EVENTS_OUTPUT = "all-events.txt";
    public static final String PRODUCT_VIEWS_OUTPUT = "product-views.txt";
    public static final String USER_EVENTS_OUTPUT = "user-events.txt";
    public static final String USER_PRODUCT_VIEWS_OUTPUT = "user-product-views.txt";
    public static final String TOP_USERS_OUTPUT = "top-users.txt";

    public static final String INPUT_FILE_PARAMETER = "input-file";
    public static final String OUTPUT_DIR_PARAMETER = "output-dir";

    public static final String DELIMITER = "|";
    public static final String ROW_DELIMITER = "\n";
    static final String DELIMITER_REGEX = "\\|";

    public static final Integer TOP_USERS_NUMBER = 5;

    public static final Integer FIELDS_NUMBER = 4;

    public static final Integer PRODUCT_ID_INDEX = 1;
    public static final Integer EVENT_NAME_INDEX = 2;
    public static final Integer USER_ID_INDEX = 3;

    public static final List<String> ALL_ACTIONS_ARRAY = Arrays.asList("view", "remove", "click", "add");

    public static final String PRODUCT_VIEWER_USER_ID = "47";
    public static final String VIEW_PRODUCT_ACTION = "view";
    public static final String ADD_PRODUCT_ACTION = "add";
    public static final String REMOVE_PRODUCT_ACTION = "remove";
    public static final String CLICK_PRODUCT_ACTION = "click";

}
