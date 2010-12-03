package me.prettyprint.cassandra.service;

import me.prettyprint.cassandra.service.CassandraClientMonitor.Counter;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.exceptions.HectorTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * We declare this pool as enum to make sure it stays a singlton in the system so clients may
 * efficiently be reused.
 *
 * @author Ran Tavory (ran@outbain.com)
 * @author zznate
 *
 */
/*package*/ class CassandraClientPoolImpl implements CassandraClientPool {

  private static final Logger log = LoggerFactory.getLogger(CassandraClientPoolImpl.class);
  /**
   * Mapping b/w the CassandraHost and the pool used to store connections to it.
   */
  private final Map<CassandraHost, CassandraClientPoolByHost> pools;

  private final CassandraClientMonitor clientMonitor;

  private CassandraHostConfigurator cassandraHostConfigurator;
  private final Cluster cluster;
  private Random random = new Random();

  private ScheduledExecutorService maintenance = Executors.newScheduledThreadPool(1);
  private long lastMaintenanceFinished = -1L;
  private Map<CassandraHost, CassandraClientPoolByHost> downPools = new ConcurrentHashMap<CassandraHost, CassandraClientPoolByHost>();

  public CassandraClientPoolImpl(CassandraClientMonitor clientMonitor) {
    log.info("Creating a CassandraClientPool");
    pools = new ConcurrentHashMap<CassandraHost, CassandraClientPoolByHost>();
    this.clientMonitor = clientMonitor;
    this.cluster = new ThriftCluster("Default Cluster", this);
    createMaintenanceCheck();
  }

  public CassandraClientPoolImpl(CassandraClientMonitor clientMonitor,
      CassandraHost[] cassandraHosts) {
    this(clientMonitor);
    log.info("Creating a CassandraClientPool with the following configuration: {}", cassandraHosts);
    for (CassandraHost cassandraHost: cassandraHosts) {
      log.debug("Maybe creating pool-by-host instance for {} at {}", cassandraHost, this);
      getPool(cassandraHost);
    }
  }

  public CassandraClientPoolImpl(CassandraClientMonitor clientMonitor,
      CassandraHostConfigurator cassandraHostConfigurator) {
    this(clientMonitor, cassandraHostConfigurator.buildCassandraHosts());
    this.cassandraHostConfigurator = cassandraHostConfigurator;
  }

  private void createMaintenanceCheck(){
    // schedule the connection fixer
    maintenance.scheduleAtFixedRate(new Runnable() {
      public void run() {
        synchronized(pools) {
          // don't run a bunch if the last one just finished
          if (lastMaintenanceFinished != -1 && System.currentTimeMillis() - lastMaintenanceFinished < 10000L)
            return;

          try {
            Set<CassandraHost> reActiveHosts = new HashSet<CassandraHost>();

            //now check all of the down ones
            for (CassandraHost host : downPools.keySet()) {
              if(!validateHostConnection(host)){
                log.warn("host " + host.getIp() + " is still down, not re-adding to connection pools");
              } else{ //add it back
                log.warn("host " + host.getIp() + " is no longer down, re-adding to connection pools");
                reActiveHosts.add(host);
              }
            }

            //check the health of all of the "healthy" ones
            Set<CassandraHost> currentClusterHosts = getKnownHosts();
            for (CassandraHost host : currentClusterHosts) {
              if(!validateHostConnection(host)){
                log.warn("detected that " + host.getIp() + " is down, removing from the connection pools");
                downPools.put(host, pools.remove(host));
              }
            }

            //add back in any hosts that were down and are up now
            for (CassandraHost host : reActiveHosts) {
              pools.put(host, downPools.remove(host));
            }

          } catch (Exception e) {
            log.error("error updating cassandra cluster based on ring", e);
          }

          lastMaintenanceFinished = System.currentTimeMillis();
        }
      }
    }, 30, 30, TimeUnit.SECONDS);
  }

  private boolean validateHostConnection(CassandraHost host) {
    TTransport transport = null;
    try {
      final TSocket thriftSocket = new TSocket(host.getIp(), cassandraHostConfigurator.getPort(), 10000);
      transport = new TFramedTransport(thriftSocket);
      TProtocol proto = new TBinaryProtocol(transport);
      org.apache.cassandra.thrift.Cassandra.Client client = new org.apache.cassandra.thrift.Cassandra.Client(proto);
      transport.open();
      client.describe_cluster_name();
      return true;
    } catch(Exception e) {
      return false;
    } finally {
      if (transport != null && transport.isOpen())
        transport.close();
    }
  }

  @Override
  public CassandraClient borrowClient() throws HectorException {
    List<CassandraHost> hosts = new ArrayList(pools.keySet());
    //sort by least active
    Collections.sort(hosts, new Comparator<CassandraHost>() {
      public int compare(CassandraHost h1, CassandraHost h2) {
        CassandraClientPoolByHost p1 = pools.get(h1);
        CassandraClientPoolByHost p2 = pools.get(h2);
        if ( p1.getNumActive() < p2.getNumActive() ) {
          return 1;
        } else if ( p1.getNumActive() > p2.getNumActive() ) {
          return -1;
        }
        return 0;
      }
    });
    return borrowClient(hosts.get(0));
  }

  @Override
  public CassandraClient borrowClient(String url, int port) throws HectorException {
    return getPool(new CassandraHost(url, port)).borrowClient();
  }

  @Override
  public CassandraClient borrowClient(CassandraHost cassandraHost) throws HectorException {
    return getPool(cassandraHost).borrowClient();
  }

  @Override
  public Set<String> getExhaustedPoolNames() {
    Set<String> hosts = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      if (pool.isExhausted()) {
        hosts.add(pool.getName());
      }
    }
    return hosts;
  }

  @Override
  public int getNumActive() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumActive();
    }
    return count;
  }

  @Override
  public int getNumBlockedThreads() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumBlockedThreads();
    }
    return count;
  }

  @Override
  public int getNumExhaustedPools() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      if (pool.isExhausted()) {
        ++count;
      }
    }
    return count;
  }

  @Override
  public int getNumIdle() {
    int count = 0;
    for (CassandraClientPoolByHost pool: pools.values()) {
      count += pool.getNumIdle();
    }
    return count;
  }

  @Override
  public int getNumPools() {
    return pools.size();
  }

  public CassandraClientPoolByHost getPool(CassandraHost cassandraHost) {
    CassandraClientPoolByHost pool = pools.get(cassandraHost);
    if (pool == null) {
      if (cassandraHostConfigurator != null) {
        cassandraHostConfigurator.applyConfig(cassandraHost);
      }
      addCassandraHost(cassandraHost);
      pool = pools.get(cassandraHost);
    }
    return pool;
  }


  @Override
  public Set<String> getPoolNames() {
    Set<String> names = new HashSet<String>();
    for (CassandraClientPoolByHost pool: pools.values()) {
      names.add(pool.getName());
    }
    return names;
  }

  @Override
  public void releaseClient(CassandraClient client) throws HectorException {
    if (client == null) {
      log.error("client is null; cannot release, there's a bug dude");
      return;
    }
    getPool(client).releaseClient(client);
  }

  @Override
  public void updateKnownHosts() throws HectorTransportException {
    synchronized(pools) {
      for (Iterator<Entry<CassandraHost, CassandraClientPoolByHost>> iterator = pools.entrySet().iterator(); iterator.hasNext();) {
        Entry<CassandraHost, CassandraClientPoolByHost> pool = iterator.next();
        if (pool.getValue().getLiveClients().isEmpty()) {
          if ( log.isInfoEnabled() ) {
            log.info("Found empty CassandraClientPoolByHost to remove: {}", pool.toString());
          }
          iterator.remove();
        }
      }
    }
  }

  @Override
  public Set<CassandraHost> getKnownHosts() {
    return Collections.unmodifiableSet(pools.keySet());
  }

  @Override
  public Set<CassandraHost> getDownHosts() {
    return Collections.unmodifiableSet(downPools.keySet());
  }

  @Override
  public void invalidateClient(CassandraClient client) {
    getPool(client).invalidateClient(client);
  }

  void reportDestroyed(CassandraClient client) {
    ((CassandraClientPoolByHostImpl) getPool(client)).reportDestroyed(client);
  }

  private CassandraClientPoolByHost getPool(CassandraClient c) {
    return getPool(c.getCassandraHost());
  }

  @Override
  public void releaseKeyspace(KeyspaceService k) throws HectorException {
    releaseClient(k.getClient());
  }

  @Override
  public CassandraClient borrowClient(String urlPort) throws HectorException {
    String url = parseHostFromUrl(urlPort);
    int port = parsePortFromUrl(urlPort);
    return borrowClient(url, port);
  }

  @Override
  public CassandraClient borrowClient(String[] clientUrls) throws HectorException {
    List<String> clients = new ArrayList<String>(Arrays.asList(clientUrls));
    while(!clients.isEmpty()) {
      int rand = random.nextInt(clients.size());
      try {
        return borrowClient(clients.get(rand));
      } catch (HectorException e) {
        if (clients.size() > 1) {
          log.warn("Unable to obtain client " + clients.get(rand) + " will try the next client");
          clientMonitor.incCounter(Counter.RECOVERABLE_LB_CONNECT_ERRORS);
          clients.remove(rand);
        } else {
          throw e;
        }
      }
    }
    // Method should never get here; an exception must have been thrown before, I'm only writing
    // this to make the compiler happy.
    return null;
  }

  private String parseHostFromUrl(String urlPort) {
    return urlPort.substring(0, urlPort.lastIndexOf(':'));
  }

  private int parsePortFromUrl(String urlPort) {
    return Integer.valueOf(urlPort.substring(urlPort.lastIndexOf(':')+1, urlPort.length()));
  }

  @Override
  public void invalidateAllConnectionsToHost(CassandraClient client) {
    getPool(client).invalidateAll();
  }

  @Override
  public CassandraClientMonitorMBean getMbean() {
    return clientMonitor;
  }

  @Override
  public String toString() {
    return "CassandraClientPoolImpl(" + pools + ")";
  }

  @Override
  public void addCassandraHost(CassandraHost cassandraHost) {
    synchronized (pools) {
      CassandraClientPoolByHost pool = pools.get(cassandraHost);
      if (pool == null) {
        pool = new CassandraClientPoolByHostImpl(cassandraHost, this, clientMonitor);
        pools.put(cassandraHost, pool);
        if ( log.isDebugEnabled() ) {
          log.debug("GenerigObjectPool created: {} {}", pool, pool.hashCode());
        }
      }
    }
  }

  @Override
  public void removeCassandraHost(CassandraHost cassandraHost) {
    synchronized (pools) {
      CassandraClientPoolByHost pool = pools.remove(cassandraHost);
      if(pool != null)
        pool.invalidateAll();
      else{
        pool = downPools.remove(cassandraHost);
        if(pool != null)
          pool.invalidateAll();
      }
    }
  }

  @Override
  public Cluster getCluster() {
    return cluster;
  }

  public void shutdown() {
    maintenance.shutdown();
  }
}
