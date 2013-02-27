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
        String hostnames = "localhost";
        String keyspaceName = "hr";
        String columnFamilyName = "cassandra_test";


        LOG.debug("Initializing AstyanaxDao...");
        AstyanaxDao adao = getAstyanaxDao(hostnames, keyspaceName, columnFamilyName);
        Map<String, String> columns = new HashMap<String, String>();
        columns.put("name", "John Smith");
        columns.put("bar", "foo");
        columns.put("Phone", "12345678");
        LOG.debug("Storing values...");
        adao.store("row1", columns);
        Map<String, String> astyanaxValues = adao.getValues("row1");
        LOG.debug("Retrieving values:" +  astyanaxValues);


        LOG.debug("Initializing HectorDao...");
        me.prettyprint.hector.api.Keyspace keyspace = createHectorKeyspace(hostnames, keyspaceName);
        HectorDao hdao = new HectorDao(keyspace, columnFamilyName);
        LOG.debug("Storing values...");
        hdao.store("row2", columns);
        Map<String, String> hectorValues = hdao.getValues("row2");
        LOG.debug("Retrieving values:" + hectorValues);
    }


    private static me.prettyprint.hector.api.Keyspace createHectorKeyspace(String hostnames, String keyspaceName) {
        CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(hostnames);

        Cluster cluster = HFactory.createCluster(DEFAULT_CLUSTER_NAME, cassandraHostConfigurator);
        me.prettyprint.hector.api.Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        return keyspace;
    }

    private static AstyanaxDao getAstyanaxDao(String hostnames, String keyspaceName, String columnFamilyName) {
        AstyanaxContext<Keyspace> context = createAstyanaxConfig(hostnames, keyspaceName, 9160);
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(),
                StringSerializer.get());
        AstyanaxDao dao = new AstyanaxDao(context, columnFamily);
        return dao;
    }

    public static AstyanaxContext<Keyspace> createAstyanaxConfig(String hostnames, String keyspaceName, int port) {
        AstyanaxContextFactory factory = new AstyanaxContextFactory();
        factory.setHostNames(hostnames);
        factory.setClusterName(DEFAULT_CLUSTER_NAME);
        factory.setKeyspace(keyspaceName);
        factory.setPort(port);
        factory.setMaxConnsPerHost(50);
        factory.setSocketTimeout(15000);
        factory.setConnectionPoolName("myConnections");

        AstyanaxContext<Keyspace> context = factory.create();

        return context;
    }

}
