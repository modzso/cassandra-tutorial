package cassandra.astyanax;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cassandra.AstyanaxContextFactory;
import cassandra.Configuration;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.entitystore.DefaultEntityManager;
import com.netflix.astyanax.entitystore.EntityManager;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * create column family sample_entity WITH key_validation_class = 'UTF8Type' and default_validation_class='UTF8Type' AND gc_grace = 86400;
 * @author Gyozo_Nyari
 *
 */
public class EntityStore<K, C> {

    private static final Logger LOG = LoggerFactory.getLogger(EntityStore.class);



	public static void main(String[] args) {
	    Configuration configuration = Configuration.getConfiguration();
		Keyspace keyspace = createKeyspace(configuration.getHostname(), configuration.getClusterName(), configuration.getKeyspace(), configuration.getPort());

        ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>("sample_entity", StringSerializer.get(),
                StringSerializer.get());

		EntityManager<SampleEntity, String> entityManager = new DefaultEntityManager.Builder<EntityStore.SampleEntity, String>()
				.withEntityType(SampleEntity.class)
				.withKeyspace(keyspace)
				.withColumnFamily(columnFamily).build();

		SampleEntity sample = new SampleEntity();
		sample.setId("sample1");
		sample.setName("sample");
		SampleEntity.Bar bar = new SampleEntity.Bar();
		bar.first = "first";
		bar.last = "last";
		sample.setBar(bar);


		LOG.debug("Storing:" + sample);
		entityManager.put(sample);

		SampleEntity stored = entityManager.get("sample1");
		LOG.debug("Retrieving:" + stored);

		//LOG.debug("Deleting sample1...");
		//entityManager.delete("sample1");
	}

	private static Keyspace createKeyspace(String hosts, String cluster, String keyspaceName, int port) {
		AstyanaxContextFactory factory = new AstyanaxContextFactory();
		factory.setHostNames(hosts);
		factory.setClusterName(cluster);
		factory.setKeyspace(keyspaceName);
		factory.setPort(port);
		factory.setMaxConnsPerHost(50);
		factory.setSocketTimeout(15000);
		factory.setConnectionPoolName("myConnections");

		AstyanaxContext<Keyspace> context = factory.create();
		Keyspace keyspace = context.getEntity();
		return keyspace;
	}

	@Entity
	public static class SampleEntity {

		@Entity
		public static class Bar {
			@Column(name = "first")
			public String first;
			@Column(name = "last")
			public String last;
			@Override
			public String toString() {
				return "Bar [first=" + first + ", last=" + last + "]";
			}
		}

		@Id
		private String id;

		@Column(name = "name")
		private String name;

		@Column(name = "bar")
		private Bar bar;


		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Bar getBar() {
			return bar;
		}

		public void setBar(Bar bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "SampleEntity [id=" + id + ", name=" + name + ", bar=" + bar
					+ "]";
		}

	}
}
