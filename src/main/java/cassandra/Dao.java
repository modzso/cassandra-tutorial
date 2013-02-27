package cassandra;

import java.util.Map;

/**
 * @author Gyozo_Nyari
 *
 */
public interface Dao {

    /**
     * Returns a row
     * @param row row key
     * @return map of columns
     */
    Map<String, String> getValues(String row);

    /**
     * Stores a row.
     * @param row row key
     * @param columns columns to be stored.
     */
    void store(String row, Map<String, String> columns);

    /**
     * Removes a row.
     * @param row row key
     */
    void remove(String row);
}
