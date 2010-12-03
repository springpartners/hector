package me.prettyprint.cassandra.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.exceptions.HectorPoolException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cluster instance the client side representation of a cassandra server cluster.
 *
 * The cluster is usually the main entry point for programs using hector. To start operating on
 * cassandra cluster you first get or create a cluster, then a keyspace operator for the keyspace
 * you're interested in and then create mutations of queries
 * <code>
 * //get a cluster:
 * Cluster cluster = getOrCreateCluster("MyCluster", "127.0.0.1:9170");
 * //get a keyspace from this cluster:
 * Keyspace ko = createKeyspace("Keyspace1", cluster);
 * //Create a mutator:
 * Mutator m = createMutator(ko);
 * // Make a mutation:
 * MutationResult mr = m.insert("key", cf, createColumn("name", "value", serializer, serializer));
 * </code>
 *
 * THREAD SAFETY: This class is thread safe.
 *
 * @author Ran Tavory
 * @author zznate
 */
public abstract class AbstractCluster implements Cluster {

  private final Logger log = LoggerFactory.getLogger(AbstractCluster.class);

  protected static final String KEYSPACE_SYSTEM = "system";

  private final CassandraClientPool pool;
  private final String name;
  private final CassandraHostConfigurator configurator;
  private TimestampResolution timestampResolution = CassandraHost.DEFAULT_TIMESTAMP_RESOLUTION;
  private final FailoverPolicy failoverPolicy;
  private final CassandraClientMonitor cassandraClientMonitor;
  private Set<String> knownClusterHosts;
  private Set<CassandraHost> knownPoolHosts;
  protected final ExceptionsTranslator xtrans;

  public AbstractCluster(String clusterName, CassandraHostConfigurator cassandraHostConfigurator) {
    pool = CassandraClientPoolFactory.INSTANCE.createNew(cassandraHostConfigurator);
    name = clusterName;
    configurator = cassandraHostConfigurator;
    failoverPolicy = FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
    cassandraClientMonitor = JmxMonitor.getInstance().getCassandraMonitor();
    xtrans = new ExceptionsTranslatorImpl();
  }

  public AbstractCluster(String clusterName, CassandraClientPool pool) {
    this.pool = pool;
    name = clusterName;
    configurator = null;
    failoverPolicy = FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
    cassandraClientMonitor = JmxMonitor.getInstance().getCassandraMonitor();
    xtrans = new ExceptionsTranslatorImpl();
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#getKnownPoolHosts(boolean)
   */
  @Override
  public Set<CassandraHost> getKnownPoolHosts(boolean refresh) {
    if (refresh || knownPoolHosts == null) {
      knownPoolHosts = pool.getKnownHosts();
      log.info("found knownPoolHosts: {}", knownPoolHosts);
    }
    return knownPoolHosts;
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#getKnownPoolHosts(boolean)
   */
  @Override
  public Set<CassandraHost> getDownPoolHosts() {
    return pool.getDownHosts();
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#getClusterHosts(boolean)
   */
  @Override
  public Set<String> getClusterHosts(boolean refresh) {
    if (refresh || knownClusterHosts == null) {
      CassandraClient client = borrowClient();
      try {
        knownClusterHosts = new HashSet<String>(buildHostNames(client.getCassandra()));
      } finally {
        releaseClient(client);
      }
    }
    return knownClusterHosts;
  }

  protected abstract Set<String> buildHostNames(Client cassandra);

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#addHost(me.prettyprint.cassandra.service.CassandraHost, boolean)
   */
  @Override
  public void addHost(CassandraHost cassandraHost, boolean skipApplyConfig) {
    if (!skipApplyConfig && configurator != null) {
      configurator.applyConfig(cassandraHost);
    }
    pool.addCassandraHost(cassandraHost);
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#addHost(me.prettyprint.cassandra.service.CassandraHost, boolean)
   */
  @Override
  public void removeHost(CassandraHost cassandraHost) {
    pool.removeCassandraHost(cassandraHost);
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#getName()
   */
  @Override
  public String getName() {
    return name;
  }

  // rest of the methods from the current CassandraCluster

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#borrowClient()
   */
  @Override
  public CassandraClient borrowClient() throws HectorPoolException {
    return pool.borrowClient();
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#releaseClient(me.prettyprint.cassandra.service.CassandraClient)
   */
  @Override
  public void releaseClient(CassandraClient client) {
    pool.releaseClient(client);
  }

  @Override
  public String toString() {
    return String.format("Cluster(%s,%s)", name, pool.toString());
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#getTimestampResolution()
   */
  @Override
  public TimestampResolution getTimestampResolution() {
    return timestampResolution;
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#setTimestampResolution(me.prettyprint.cassandra.service.TimestampResolution)
   */
  @Override
  public Cluster setTimestampResolution(TimestampResolution timestampResolution) {
    this.timestampResolution = timestampResolution;
    return this;
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#createTimestamp()
   */
  @Override
  public long createTimestamp() {
    return timestampResolution.createTimestamp();
  }


  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#describeKeyspaces()
   */
  @Override
  public Set<String> describeKeyspaces() throws HectorException {
    Operation<Set<String>> op = new Operation<Set<String>>(OperationType.META_READ) {
      @Override
      public Set<String> execute(Cassandra.Client cassandra) throws HectorException {
        try {
          return cassandra.describe_keyspaces();
        } catch (Exception e) {
          throw xtrans.translate(e);
        }
      }
    };
    operateWithFailover(op);
    return op.getResult();
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#describeClusterName()
   */
  @Override
  public String describeClusterName() throws HectorException {
    Operation<String> op = new Operation<String>(OperationType.META_READ) {
      @Override
      public String execute(Cassandra.Client cassandra) throws HectorException {
        try {
          return cassandra.describe_cluster_name();
        } catch (Exception e) {
          throw xtrans.translate(e);
        }
      }
    };
    operateWithFailover(op);
    return op.getResult();
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#describeThriftVersion()
   */
  @Override
  public String describeThriftVersion() throws HectorException {
    Operation<String> op = new Operation<String>(OperationType.META_READ) {
      @Override
      public String execute(Cassandra.Client cassandra) throws HectorException {
        try {
          return cassandra.describe_version();
        } catch (Exception e) {
          throw xtrans.translate(e);
        }
      }
    };
    operateWithFailover(op);
    return op.getResult();
  }

  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#describeKeyspace(java.lang.String)
   */
  @Override
  public Map<String, Map<String, String>> describeKeyspace(final String keyspace)
  throws HectorException {
    Operation<Map<String, Map<String, String>>> op = new Operation<Map<String, Map<String, String>>>(
        OperationType.META_READ) {
      @Override
      public Map<String, Map<String, String>> execute(Cassandra.Client cassandra)
      throws HectorException {
        try {
          return cassandra.describe_keyspace(keyspace);
        } catch (org.apache.cassandra.thrift.NotFoundException nfe) {
          setException(xtrans.translate(nfe));
          return null;
        } catch (Exception e) {
          throw xtrans.translate(e);
        }
      }
    };
    operateWithFailover(op);
    return op.getResult();
  }


  /* (non-Javadoc)
   * @see me.prettyprint.cassandra.service.Cluster#getClusterName()
   */
  @Override
  public String getClusterName() throws HectorException {
    log.info("in execute with client");
    Operation<String> op = new Operation<String>(OperationType.META_READ) {
      @Override
      public String execute(Cassandra.Client cassandra) throws HectorException {
        try {
          log.info("in execute with client {}", cassandra);
          return cassandra.describe_cluster_name();
        } catch (Exception e) {
          throw xtrans.translate(e);
        }

      }
    };
    operateWithFailover(op);
    return op.getResult();
  }

  protected void operateWithFailover(Operation<?> op) throws HectorException {
    CassandraClient client = null;
    try {
      client = borrowClient();
      FailoverOperator operator = new FailoverOperator(failoverPolicy,
          cassandraClientMonitor, client, pool, null);
      client = operator.operate(op);
    } finally {
      try {
        releaseClient(client);
      } catch (Exception e) {
        log.error("Unable to release a client", e);
      }
    }
  }

  public void shutdown() {
    pool.shutdown();
  }
}
