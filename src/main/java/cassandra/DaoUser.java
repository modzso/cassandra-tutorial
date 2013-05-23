package cassandra;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;


import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * CLI: create column family cassandra_test WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
 * @author Gyozo_Nyari
 *
 */
public class DaoUser {

    private static final String DEFAULT_CLUSTER_NAME = "default";
    private static final Logger LOG = LoggerFactory.getLogger(DaoUser.class);

	/**
     * @param args
     */
    public static void main(String[] args) {
        String columnFamilyName = "cassandra_test";


        LOG.debug("Initializing AstyanaxDao...");
        Configuration configuration = Configuration.getConfiguration();
        AstyanaxDao adao = getAstyanaxDao(columnFamilyName);
        Map<String, String> columns = new HashMap<String, String>();
        columns.put("name", "John Smith");
        columns.put("bar", "foo");
        columns.put("Phone", "12345678");
        LOG.debug("AstyanaxDao - Storing values...");
        adao.store("row1", columns);
        Map<String, String> astyanaxValues = adao.getValues("row1");
        LOG.debug("AstyanaxDao - Retrieving values:" +  astyanaxValues);


        LOG.debug("Initializing HectorDao...");
        me.prettyprint.hector.api.Keyspace keyspace = createHectorKeyspace(configuration.getHostname(), configuration.getKeyspace());
        HectorDao hdao = new HectorDao(keyspace, columnFamilyName);
        LOG.debug("HectorDao - Storing values...");
        hdao.store("row2", columns);
        Map<String, String> hectorValues = hdao.getValues("row2");
        LOG.debug("HectorDao - Retrieving values:" + hectorValues);

        LOG.debug("Finished.");
    }


    private static me.prettyprint.hector.api.Keyspace createHectorKeyspace(String hostnames, String keyspaceName) {
        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(hostnames);

        Cluster cluster = HFactory.createCluster(DEFAULT_CLUSTER_NAME, cassandraHostConfigurator);
        me.prettyprint.hector.api.Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        return keyspace;
    }

    private static AstyanaxDao getAstyanaxDao(String columnFamilyName) {
        AstyanaxContext<Keyspace> context = createAstyanaxConfig();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(),
                StringSerializer.get());
        AstyanaxDao dao = new AstyanaxDao(context, columnFamily);
        return dao;
    }

    public static AstyanaxContext<Keyspace> createAstyanaxConfig() {
        Configuration configuration = Configuration.getConfiguration();
        AstyanaxContextFactory factory = new AstyanaxContextFactory();
        factory.setHostNames(configuration.getHostname());
        factory.setClusterName(configuration.getClusterName());
        factory.setKeyspace(configuration.getKeyspace());
        factory.setPort(configuration.getPort());
        factory.setMaxConnsPerHost(configuration.getPort());
        factory.setSocketTimeout(configuration.getSocketTimeout());
        factory.setConnectionPoolName(configuration.getConnectionPoolName());

        AstyanaxContext<Keyspace> context = factory.create();

        return context;
    }

}
