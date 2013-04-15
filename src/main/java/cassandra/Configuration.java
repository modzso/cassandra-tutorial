package cassandra;

/**
 * @author Gyozo_Nyari
 *
 */
public class Configuration {

    private String hostname = "localhost";
    private String keyspace = "hr";
    private String clusterName = "default";
    private int port = 9160;
    private int maxConnsPerHost = 50;
    private int socketTimeout = 15000;
    private String connectionPoolName = "MyConnectionPool";

    private static Configuration configuration;

    public static Configuration getConfiguration() {
        if (configuration == null) {
            configuration = new Configuration();
        }
        return configuration;
    }

    public String getHostname() {
        return hostname;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getPort() {
        return port;
    }

    public int getMaxConnsPerHost() {
        return maxConnsPerHost;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public String getConnectionPoolName() {
        return connectionPoolName;
    }

}
