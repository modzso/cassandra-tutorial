package cassandra;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * @author Gyozo_Nyari
 *
 */
public class AstyanaxContextFactory {

    private static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 5;
    private AstyanaxConfiguration astyanaxConfiguration;
    private ConnectionPoolConfiguration connectionPoolConfiguration;
    private ConnectionPoolMonitor connectionPoolMonitor;

    private String hostNames;
    private String clusterName;
    private String keyspace;
    private int port;
    private int socketTimeout;
    private int maxConnsPerHost;
    private String connectionPoolName;

    public AstyanaxContext<Keyspace> create() {
        astyanaxConfiguration = getDefaultConfiguration();
        connectionPoolConfiguration = getDefaultConnectionPoolConfiguration(hostNames, port);
        connectionPoolMonitor = getDefaultConnectionPoolMonitor();

        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(clusterName).forKeyspace(keyspace)
                .withAstyanaxConfiguration(astyanaxConfiguration).withConnectionPoolConfiguration(connectionPoolConfiguration)
                .withConnectionPoolMonitor(connectionPoolMonitor).buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        return context;
    }

    private AstyanaxConfiguration getDefaultConfiguration() {
        return new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE);
    }

    private ConnectionPoolConfiguration getDefaultConnectionPoolConfiguration(String host, int port) {
        return new ConnectionPoolConfigurationImpl(connectionPoolName).setSocketTimeout(socketTimeout).setPort(port)
                .setMaxConnsPerHost(maxConnsPerHost).setSeeds(host + ":" + port).setMaxBlockedThreadsPerHost(20);
    }

    private ConnectionPoolMonitor getDefaultConnectionPoolMonitor() {
        return new Slf4jConnectionPoolMonitorImpl();
    }

    public void setAstyanaxConfiguration(AstyanaxConfiguration astyanaxConfiguration) {
        this.astyanaxConfiguration = astyanaxConfiguration;
    }

    public void setConnectionPoolConfiguration(ConnectionPoolConfiguration connectionPoolConfiguration) {
        this.connectionPoolConfiguration = connectionPoolConfiguration;
    }

    public void setConnectionPoolMonitor(ConnectionPoolMonitor connectionPoolMonitor) {
        this.connectionPoolMonitor = connectionPoolMonitor;
    }

    public void setHostNames(String hostNames) {
        this.hostNames = hostNames;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public void setMaxConnsPerHost(int maxConnsPerHost) {
        this.maxConnsPerHost = maxConnsPerHost;
    }

    public void setConnectionPoolName(String connectionPoolName) {
        this.connectionPoolName = connectionPoolName;
    }

}
