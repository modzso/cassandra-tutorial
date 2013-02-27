package cassandra;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * CLI: create column family auth_failures WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
 *
 * @author Gyozo_Nyari
 *
 */
public class AstyanaxCountingDao implements CountingDao {

    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxCountingDao.class);
    private final ConcurrentMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<String, ReentrantLock>();

    private final Keyspace keyspace;
    private final ColumnFamily<String, String> columnFamily;
    private int ttl = 60;


    public AstyanaxCountingDao(final AstyanaxContext<Keyspace> astyanaxContext, final ColumnFamily<String, String> columnFamily) {
        super();
        this.keyspace = astyanaxContext.getEntity();
        this.columnFamily = columnFamily;
    }

    @Override
    public int getNumberOfAuthenticationFailures(final String userId, final String credential) {
        int attempts = 0;
        try {
            lock(userId);
            Column<String> result = keyspace.prepareQuery(columnFamily).getKey(userId).getColumn(credential).execute().getResult();
            attempts = result.getIntegerValue();
        } catch (NotFoundException e) {
            LOG.debug("Column was not found!");             // thrift specific
        } catch (ConnectionException e) {
            throw new CassandraException("getNumberOfAuthenticationFailures", e);
        } finally {
            unlock(userId);
        }
        return attempts;
    }



    @Override
	public Map<String, Integer> getNumberOfAuthenticationFailures(String userId) {
		Map<String, Integer> authFailures = new HashMap<>();
		try {
			lock(userId);
            ColumnList<String> result = keyspace.prepareQuery(columnFamily).getKey(userId).execute().getResult();
            for (Iterator<Column<String>> i = result.iterator(); i.hasNext();) {
            	Column<String> column = i.next();
            	authFailures.put(column.getName(), column.getIntegerValue());
            }
		} catch (ConnectionException e) {
			throw new CassandraException("getNumberOfAuthenticationFailures", e);
		} finally {
			unlock(userId);
		}
		return authFailures;
	}

	@Override
    public void setNumberOfAuthenticationFailures(final String userId, final String credential, final int value) {
        try {
            lock(userId);
            MutationBatch mutation = keyspace.prepareMutationBatch();
            mutation.withRow(columnFamily, userId).putColumn(credential, value, ttl);
            mutation.execute();
        } catch (ConnectionException e) {
            throw new CassandraException("setNumberOfAuthenticationFailures", e);
        } finally {
            unlock(userId);
        }
    }

    @Override
    public void incrementNumberOfAuthenticationFailures(final String userId, final String credential) {
        int value = 0;
        try {
            lock(userId);
            ColumnList<String> result = keyspace.prepareQuery(columnFamily).getKey(userId).execute().getResult();
            if (!result.isEmpty()) {
                value = result.getColumnByName(credential).getIntegerValue();
            }
            keyspace.prepareColumnMutation(columnFamily, userId, credential).putValue(++value, ttl).execute();
        } catch (ConnectionException e) {
            throw new CassandraException("incrementNumberOfAuthenticationFailures", e);
        } finally {
            unlock(userId);
        }
    }

    private void lock(final String key) {
        ReentrantLock oldLock = lockMap.get(key);
        if (oldLock != null && oldLock.isHeldByCurrentThread()) {
            oldLock.lock();
            return;
        }
        ReentrantLock newLock = new ReentrantLock();
        newLock.lock();
        while ((oldLock = lockMap.putIfAbsent(key, newLock)) != null) {
            oldLock.lock();
            oldLock.unlock();
        }
        return;
    }

    private void unlock(final String key) {
        ReentrantLock lock = lockMap.get(key);
        if (lock == null) {
            throw new IllegalMonitorStateException();
        }
        if (lock.getHoldCount() == 1) {
            lockMap.remove(key);
        }
        lock.unlock();
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }


    public static void main(String[] args) {
        AstyanaxContextFactory factory = new AstyanaxContextFactory();
        factory.setHostNames("localhost");
        factory.setClusterName("default");
        factory.setKeyspace("hr");
        factory.setPort(9160);
        factory.setMaxConnsPerHost(50);
        factory.setSocketTimeout(15000);
        factory.setConnectionPoolName("myConnections");

        AstyanaxContext<Keyspace> context = factory.create();
        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>("auth_failures", StringSerializer.get(),
                StringSerializer.get());
        AstyanaxCountingDao dao = new AstyanaxCountingDao(context, columnFamily);

        LOG.debug("Item1:" + dao.getNumberOfAuthenticationFailures("item1"));

        dao.incrementNumberOfAuthenticationFailures("user1", "item1");
        LOG.debug("Item1:" + dao.getNumberOfAuthenticationFailures("user1"));
        dao.setNumberOfAuthenticationFailures("user1", "item1", 0);

        int maxItems = 20;
        for (int i = 0; i < maxItems; i++) {
            dao.setNumberOfAuthenticationFailures("user1", "item" + i, 0);
        }

        for (int i = 0; i < maxItems; i++) {
            LOG.debug("Final: item" + i + " :" + dao.getNumberOfAuthenticationFailures("item" + i));
        }

    }
}
