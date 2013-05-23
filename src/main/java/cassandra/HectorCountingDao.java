package cassandra;


import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;


import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * CLI: create column family auth_failures WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
 * @author Gyozo_Nyari
 *
 */
public class HectorCountingDao implements CountingDao {

    private static final String CREDENTIAL3 = "credential3";

    private static final String ROW_3 = "3";

    private static final String CREDENTIAL2 = "credential2";

    private static final Logger LOG = LoggerFactory.getLogger(HectorCountingDao.class);

    private String columnFamilyName;
    private Keyspace keyspace;

    private ColumnFamilyTemplate<String, String> template;
    private Serializer<String> keySerializer = StringSerializer.get();
    private Serializer<String> columnNameSerializer = StringSerializer.get();
    private Serializer<Integer> valueSerializer = IntegerSerializer.get();
    private int ttl;
    private String prefix;

    /**
     * Constructor.
     */
    public HectorCountingDao() {
        super();
    }

    public void initialize() throws Exception {
        Assert.notNull(keyspace, "Keyspace not set!");
        Assert.notNull(columnFamilyName, "ColumnFamilyName is not set!");

        template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamilyName, keySerializer, columnNameSerializer);
    }

    @Override
    public int getNumberOfAuthenticationFailures(final String userId, final String credentials) {
        Assert.notNull(userId, "UserId can't be null!");
        Assert.notNull(credentials, "Credentials can't be null!");

        int authFailures = 0;
        try {
            ColumnFamilyResult<String, String> result = template.queryColumns(getPrefixedKey(userId));

            authFailures = result.getInteger(credentials);
        } catch (HectorException e) {
            throw new CassandraException("getNumberOfAuthenticationFailures failed!", e);
        }
        return authFailures;
    }

    @Override
    public Map<String, Integer> getNumberOfAuthenticationFailures(final String userId) {
        Assert.notNull(userId, "UserId can't be null!");
        Map<String, Integer> authFailuresMap = new HashMap<String, Integer>();
        try {
            ColumnFamilyResult<String, String> result = template.queryColumns(getPrefixedKey(userId));
            if (result.hasResults()) {

            for (String columnName : result.getColumnNames()) {
                Integer v = result.getInteger(columnName);
                authFailuresMap.put(columnName, v);
            }
            }
        } catch (HectorException e) {
            throw new CassandraException("getNumberOfAuthenticationFailures failed!", e);
        }
        return authFailuresMap;
    }

    @Override
    public void setNumberOfAuthenticationFailures(final String userId, final String credentials, final int value) {
        Assert.notNull(userId, "UserId can't be null!");
        Assert.notNull(credentials, "Credentials can't be null!");

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, keySerializer);
            mutator.insert(getPrefixedKey(userId), columnFamilyName, HFactory.createColumn(credentials, value, ttl, columnNameSerializer, valueSerializer));
        } catch (HectorException e) {
            throw new CassandraException("setNumberOfAuthenticationFailures has failed!", e);
        }
    }

    @Override
    public void incrementNumberOfAuthenticationFailures(final String userId, final String credentials) {
        Assert.notNull(userId, "UserId can't be null!");
        Assert.notNull(credentials, "Credentials can't be null!");

        int value = 0;

        HColumn<String, Integer> column = getColumn(userId, credentials);
        if (column != null) {
            Integer v = column.getValue();
            value = v != null ? v.intValue() : 0;
        }
        value++;

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, keySerializer);
            mutator.addInsertion(getPrefixedKey(userId), columnFamilyName, HFactory.createColumn(credentials, value, ttl, columnNameSerializer, valueSerializer));
            mutator.execute();
        } catch (HectorException e) {
            throw new CassandraException("incrementNumberOfAuthenticationFailures has failed!", e);
        }
    }

    private HColumn<String, Integer> getColumn(final String userId, final String credentials) {
        ColumnQuery<String, String, Integer> columnQuery = HFactory.createColumnQuery(keyspace, keySerializer, columnNameSerializer, valueSerializer);
        columnQuery.setColumnFamily(columnFamilyName).setKey(getPrefixedKey(userId)).setName(credentials);
        QueryResult<HColumn<String, Integer>> result = columnQuery.execute();

        HColumn<String, Integer> column = result.get();
        return column;
    }

    private String getPrefixedKey(final String key) {
        return prefix != null ? String.format("%s:%s", prefix, key) : key;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public void setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public static void main(String[] args) throws Exception {
        LOG.debug("Starting Hector counting dao...");
        Configuration configuration = Configuration.getConfiguration();
        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(configuration.getHostname());

        Cluster cluster = HFactory.createCluster("default", cassandraHostConfigurator);
        Keyspace keyspace = HFactory.createKeyspace(configuration.getKeyspace(), cluster);

        HectorCountingDao dao = new HectorCountingDao();
        dao.setKeyspace(keyspace);
        dao.setColumnFamilyName("auth_failures");
        dao.setPrefix("DEV");
        dao.setTtl(3600);

        dao.initialize();

        LOG.debug("Incrementing row: {} column: {}", ROW_3, CREDENTIAL2);
        dao.incrementNumberOfAuthenticationFailures(ROW_3, CREDENTIAL2);

        LOG.debug("incrementing row: {} column: {}", ROW_3, CREDENTIAL3);
        dao.incrementNumberOfAuthenticationFailures(ROW_3, CREDENTIAL3);

        Map<String, Integer> values = dao.getNumberOfAuthenticationFailures(ROW_3);
        LOG.debug("Retrieved values: {}", values);

        LOG.debug("Finished...");
    }

}
