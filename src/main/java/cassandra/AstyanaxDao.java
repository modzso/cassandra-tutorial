package cassandra;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;

/**
 * Cassandra dao implementation with astyanax.
 * @author Gyozo_Nyari
 *
 */
public class AstyanaxDao implements Dao {

    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxDao.class);

    private final Keyspace keyspace;
    private final ColumnFamily<String, String> columnFamily;

    /**
     * Constructor.
     * @param astyanaxContext context
     * @param columnFamily column family
     */
    public AstyanaxDao(final AstyanaxContext<Keyspace> astyanaxContext, final ColumnFamily<String, String> columnFamily) {
        super();
        this.keyspace = astyanaxContext.getEntity();
        this.columnFamily = columnFamily;
    }

    @Override
    public Map<String, String> getValues(final String row) {
        Assert.notNull(row, "Row cant be null!");
        Map<String, String> columns = new HashMap<String, String>();
        try {
            ColumnList<String> result = keyspace.prepareQuery(columnFamily).getKey(row).execute().getResult();
            if (!result.isEmpty()) {
                for (Iterator<Column<String>> i = result.iterator(); i.hasNext();) {
                    Column<String> column = i.next();
                    columns.put(column.getName(), column.getStringValue());
                }
            }
        } catch (ConnectionException e) {
            LOG.error("getValues failed for row {}!", new Object[] {row}, e);
        }
        return columns;
    }

    @Override
    public void store(final String row, final Map<String, String> columns) {
        Assert.notNull(row, "Row cant be null!");
        Assert.notNull(columns, "Columns cant be null!");
        try {
            MutationBatch mutation = keyspace.prepareMutationBatch();
            ColumnListMutation<String> columnList = mutation.withRow(columnFamily, row);
            for (String key : columns.keySet()) {
                String value = columns.get(key);
                columnList.putColumn(key, value, null);
            }
            mutation.execute();
        } catch (ConnectionException e) {
            LOG.error("storing row {} failed!", new Object[] {row}, e);
        }
    }

    @Override
    public void remove(final String row) {
        Assert.notNull(row, "Row cant be null!");
        ColumnListMutation<String> mutation = keyspace.prepareMutationBatch().withRow(columnFamily, row);
        mutation.delete();
    }

}
