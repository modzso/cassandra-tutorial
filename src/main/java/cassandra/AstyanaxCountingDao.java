package cassandra;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * CLI: create column family payment_cvv_attempt WITH key_validation_class =
 * 'UTF8Type' and default_validation_class='IntegerType';
 *
 * @author Gyozo_Nyari
 *
 */
public class AstyanaxCountingDao {

    private static final Logger LOG = LoggerFactory.getLogger(AstyanaxCountingDao.class);
    private final ConcurrentMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<String, ReentrantLock>();

    private final Keyspace keyspace;
    private final ColumnFamily<String, Integer> columnFamily;
    private int ttl = 60;


    public AstyanaxCountingDao(final AstyanaxContext<Keyspace> astyanaxContext, final ColumnFamily<String, Integer> columnFamily) {
        super();
        this.keyspace = astyanaxContext.getEntity();
        this.columnFamily = columnFamily;
    }

    public int getNumberOfAuthenticationFailures(final String paymentItemId) {
        int attempts = 0;
        try {
            lock(paymentItemId);
            Column<Integer> result = keyspace.prepareQuery(columnFamily).getKey(paymentItemId).getColumn(1).execute().getResult();
            attempts = result.getIntegerValue();
        } catch (NotFoundException e) {
            LOG.debug("Column was not found!");             // thrift specific
        } catch (ConnectionException e) {
            throw new CassandraException("getNumberOfAuthenticationFailures", e);
        } finally {
            unlock(paymentItemId);
        }
        return attempts;
    }

    public void setNumberOfAuthenticationFailures(final String paymentItemId, final int value) {
        try {
            lock(paymentItemId);
            MutationBatch mutation = keyspace.prepareMutationBatch();
            mutation.withRow(columnFamily, paymentItemId).putColumn(1, value, ttl);
            mutation.execute();
        } catch (ConnectionException e) {
            throw new CassandraException("setNumberOfAuthenticationFailures", e);
        } finally {
            unlock(paymentItemId);
        }
    }

    public void incrementNumberOfAuthenticationFailures(final String paymentItemId) {
        int value = 0;
        try {
            lock(paymentItemId);
            ColumnList<Integer> result = keyspace.prepareQuery(columnFamily).getKey(paymentItemId).execute().getResult();
            if (!result.isEmpty()) {
                value = result.getColumnByIndex(0).getIntegerValue();
            }
            keyspace.prepareColumnMutation(columnFamily, paymentItemId, 1).putValue(++value, ttl).execute();
        } catch (ConnectionException e) {
            throw new CassandraException("incrementNumberOfAuthenticationFailures", e);
        } finally {
            unlock(paymentItemId);
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

    private static class IncrementTask implements Runnable {
        private final AstyanaxCountingDao dao;
        private final String paymentItemId;

        public IncrementTask(AstyanaxCountingDao dao, String paymentItemId) {
            this.dao = dao;
            this.paymentItemId = paymentItemId;
        }

        @Override
        public void run() {
            LOG.debug(String.format("Before increment [%s]:[%d]", paymentItemId, dao.getNumberOfAuthenticationFailures(paymentItemId)));
            dao.incrementNumberOfAuthenticationFailures(paymentItemId);
            LOG.debug(String.format("After increment [%s]:[%d]", paymentItemId, dao.getNumberOfAuthenticationFailures(paymentItemId)));
        }

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
        ColumnFamily<String, Integer> columnFamily = new ColumnFamily<String, Integer>("payment_cvv_attempt", StringSerializer.get(),
                IntegerSerializer.get());
        AstyanaxCountingDao dao = new AstyanaxCountingDao(context, columnFamily);

        LOG.debug("Item1:" + dao.getNumberOfAuthenticationFailures("item1"));

        dao.incrementNumberOfAuthenticationFailures("item1");
        LOG.debug("Item1:" + dao.getNumberOfAuthenticationFailures("item1"));
        dao.setNumberOfAuthenticationFailures("item1", 0);

        int maxItems = 20;
        for (int i = 0; i < maxItems; i++) {
            dao.setNumberOfAuthenticationFailures("item" + i, 0);
        }

        ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < maxItems; j++) {
                String id = "item" + j;

                IncrementTask task = new IncrementTask(dao, id);
                executorService.execute(task);
            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOG.debug("Awaken");
        }

        for (int i = 0; i < maxItems; i++) {
            LOG.debug("Final: item" + i + " :" + dao.getNumberOfAuthenticationFailures("item" + i));
        }


        System.out.println("Shutting down...");         // NOPMD
        executorService.shutdown();
    }
}
