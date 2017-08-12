import java.util.Arrays;

/**
 * Created by vinay on 4/16/17.
 */
public class ColumnParser {

    public static int getTheIndexOfTheColumn(String[] row, String columnName)
    {
        return Arrays.asList(row).indexOf(columnName);
    }
}
