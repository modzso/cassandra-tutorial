/**
 *
 */
package cassandra.composite;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * @author Gyozo_Nyari
 *
 */
public class CompositeDao<CK> {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeDao.class);
    private Keyspace keyspace;
    private AstyanaxContext<Keyspace> astyanaxContext;
    private Class<CK> compositeKeyClazz;

    public CompositeDao(String host, String keyspace, Class<CK> compositeKeyClass) {
        try {
            this.astyanaxContext = new AstyanaxContext.Builder()
                    .forCluster("ClusterName")
                    .forKeyspace(keyspace)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                    .withConnectionPoolConfiguration(
                            new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(1)
                                    .setSeeds(host)).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            this.astyanaxContext.start();
            this.keyspace = this.astyanaxContext.getEntity();
            // test the connection
            this.keyspace.describeKeyspace();
            this.compositeKeyClazz = compositeKeyClass;
        } catch (Throwable e) {
            LOG.warn("Preparation failed.", e);
            throw new RuntimeException("Failed to prepare CassandraBolt", e);
        }
    }

    public void cleanup() {
        this.astyanaxContext.shutdown();
    }

    /**
     * Writes columns.
     */
    public void write(String columnFamilyName, String rowKey, Map<String, String> columns) throws ConnectionException {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            mutation.withRow(columnFamily, rowKey).putColumn(entry.getKey(), entry.getValue(), null);
        }
        mutation.execute();
    }

    /**
     * Writes compound/composite columns.
     */
    public void writeComposite(String columnFamilyName, String rowKey, CK compositeKey, byte[] value) throws ConnectionException {
        AnnotatedCompositeSerializer<CK> entitySerializer = new AnnotatedCompositeSerializer<CK>(compositeKeyClazz);
        MutationBatch mutation = keyspace.prepareMutationBatch();
        ColumnFamily<String, CK> columnFamily = new ColumnFamily<String, CK>(columnFamilyName,
                StringSerializer.get(), entitySerializer);
        mutation.withRow(columnFamily, rowKey).putColumn(compositeKey, value, null);
        mutation.execute();
    }

    /**
     * Fetches an entire row.
     */
    public ColumnList<String> read(String columnFamilyName, String rowKey) throws ConnectionException {
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName,
                StringSerializer.get(), StringSerializer.get());
        OperationResult<ColumnList<String>> result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();
        return result.getResult();
    }

    /**
     * Fetches an entire row using composite keys
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public ColumnList<CK> readComposite(String columnFamilyName, String rowKey) throws ConnectionException {
        AnnotatedCompositeSerializer<CK> entitySerializer = new AnnotatedCompositeSerializer<CK>(compositeKeyClazz);
        ColumnFamily<String, CK> columnFamily = new ColumnFamily<String, CK>(columnFamilyName,
                StringSerializer.get(), entitySerializer);
        OperationResult<ColumnList<CK>> result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).execute();
        return result.getResult();
    }

    /**
     * Fetches an entire row using composite keys
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Column<CompositeKey> readComposite(String columnFamilyName, String rowKey, CompositeKey ck) throws ConnectionException {
        AnnotatedCompositeSerializer<CompositeKey> entitySerializer = new AnnotatedCompositeSerializer<CompositeKey>(CompositeKey.class);
        ColumnFamily<String, CompositeKey> columnFamily = new ColumnFamily<String, CompositeKey>(columnFamilyName,
                StringSerializer.get(), entitySerializer);
        OperationResult<Column<CompositeKey>> result = this.keyspace.prepareQuery(columnFamily).getKey(rowKey).getColumn(ck).execute();
        return result.getResult();
    }

    public static void main(String[] args) throws Exception {
        CompositeDao<CompositeKey> dao = new CompositeDao<>("localhost", "examples", CompositeKey.class);

        CompositeKey ck1 = new CompositeKey();
        ck1.fileId = 1L;

        dao.writeComposite("Files", "1", ck1, "This is file1".getBytes());

        CompositeKey ck1b = new CompositeKey();
        ck1b.fileId = 1L;
        ck1b.field = "Name";

        dao.writeComposite("Files", "1", ck1b, "201300122.txt".getBytes());

        CompositeKey ck2 = new CompositeKey();
        ck2.fileId = 1L;
        ck2.trackId = 1L;

        dao.writeComposite("Files", "1", ck2, "This is file1, track1".getBytes());

        CompositeKey ck3 = new CompositeKey();
        ck3.fileId = 1L;
        ck3.trackId = 1L;
        ck3.field = "Name";

        dao.writeComposite("Files", "1", ck3, "This is file1, track1, name".getBytes());

        CompositeKey ck4 = new CompositeKey();
        ck4.fileId = 1L;
        ck4.trackId = 1L;
        ck4.field = "Phone";

        dao.writeComposite("Files", "1", ck4, "+362012345678".getBytes());

        ColumnList<CompositeKey> cl = dao.readComposite("Files", "1");
        for (CompositeKey key : cl.getColumnNames()) {
            String value = cl.getStringValue(key, null);
            LOG.debug("{}:{}:{} = {}", new Object[] {key.trackId, key.fileId, key.field, value});
        }

        Column<CompositeKey> column = dao.readComposite("Files", "1", ck2);

        LOG.debug("{}:{}:{} = {}", new Object[] {column.getName().trackId, column.getName().fileId, column.getName().field, column.getStringValue()});
    }

}
