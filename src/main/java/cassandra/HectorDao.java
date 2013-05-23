package cassandra;

import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.springframework.util.Assert;

/**
 * Implementation of the {@link Dao} interface with Hector api.
 * @author Gyozo_Nyari
 */
public class HectorDao implements Dao {

    private final Keyspace keyspace;
    private final String columnFamilyName;
    private final Serializer<String> keySerializer = StringSerializer.get();
    private final Serializer<String> columnNameSerializer = StringSerializer.get();
    private final Serializer<String> valueSerializer = StringSerializer.get();

    private final ColumnFamilyTemplate<String, String> template;

    /**
     * Constructor.
     * @param keyspace keyspace to be used.
     * @param columnFamilyName name of the column family.
     */
    public HectorDao(final Keyspace keyspace, final String columnFamilyName) {
        this.keyspace = keyspace;
        this.columnFamilyName = columnFamilyName;
        this.template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamilyName, keySerializer, columnNameSerializer);
    }

    @Override
    public Map<String, String> getValues(final String row) {
        Assert.notNull(row, "Row can't be null!");
        Map<String, String> columns = new HashMap<String, String>();
        try {
            ColumnFamilyResult<String, String> result = template.queryColumns(row);
            if (result.hasResults()) {

                for (String columnName : result.getColumnNames()) {
                    String value = result.getString(columnName);
                    columns.put(columnName, value);
                }
            }
        } catch (HectorException e) {
            throw new CassandraException("getNumberOfAuthenticationFailures failed!", e);
        }
        return columns;
    }

    @Override
    public void store(final String row, final Map<String, String> columns) {
        Assert.notNull(row, "Row can't be null!");
        Assert.notNull(columns, "Columns can't be null!");

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, keySerializer);
            for (String columnName : columns.keySet()) {
                String value = columns.get(columnName);
                mutator.insert(row, columnFamilyName, HFactory.createColumn(columnName, value, 0L, columnNameSerializer, valueSerializer));
            }
        } catch (HectorException e) {
            throw new CassandraException("setNumberOfAuthenticationFailures has failed!", e);
        }
    }

    @Override
    public void remove(final String row) {
        Assert.notNull(row, "Row cant be null!");
        template.deleteRow(row);
    }

}
