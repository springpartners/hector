package me.prettyprint.cassandra.model;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.utils.Assert;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.ConsistencyLevelPolicy.OperationType;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;

/**
 *
 * @author Ran Tavory
 *
 */
public class ExecutingKeyspace implements Keyspace {

  private ConsistencyLevelPolicy consistencyLevelPolicy;

  private final Cluster cluster;
  private final String keyspace;
  private CassandraClient.FailoverPolicy failoverPolicy;

  public ExecutingKeyspace(String keyspace, Cluster cluster,
      ConsistencyLevelPolicy consistencyLevelPolicy) {
    this(keyspace, cluster, consistencyLevelPolicy, null);
  }

  public ExecutingKeyspace(String keyspace, Cluster cluster,
      ConsistencyLevelPolicy consistencyLevelPolicy, CassandraClient.FailoverPolicy failoverPolicy) {
    Assert.noneNull(keyspace, cluster, consistencyLevelPolicy);
    this.keyspace = keyspace;
    this.cluster = cluster;
    this.consistencyLevelPolicy = consistencyLevelPolicy;
    this.failoverPolicy = failoverPolicy;
  }

  @Override
  public void setConsistencyLevelPolicy(ConsistencyLevelPolicy cp) {
    this.consistencyLevelPolicy = cp;
  }

  @Override
  public Cluster getCluster() {
    return cluster;
  }

  @Override
  public String toString() {
    return "ExecutingKeyspace(" + keyspace +"," + cluster + ")";
  }

  @Override
  public long createTimestamp() {
    return cluster.createTimestamp();
  }

  public <T> ExecutionResult<T> doExecute(KeyspaceOperationCallback<T> koc) throws HectorException {
    CassandraClient c = null;
    KeyspaceService ks = null;
    try {
        c = cluster.borrowClient();
        if (failoverPolicy != null)
          ks = c.getKeyspace(keyspace, consistencyLevelPolicy.get(OperationType.READ), failoverPolicy);
        else
          ks = c.getKeyspace(keyspace, consistencyLevelPolicy.get(OperationType.READ));

        return koc.doInKeyspaceAndMeasure(ks);
    } finally {
      if ( ks != null ) {
        cluster.releaseClient(ks.getClient());
      }
    }
  }
}
