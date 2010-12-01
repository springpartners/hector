package me.prettyprint.cassandra.service;

import me.prettyprint.hector.api.exceptions.HTimedOutException;
import me.prettyprint.hector.api.exceptions.HUnavailableException;
import me.prettyprint.hector.api.exceptions.HectorTransportException;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What should the client do if a call to cassandra node fails and we suspect that the node is
 * down. (e.g. it's a communication error, not an application error).
 *
 * {@value #FAIL_FAST} will return the error as is to the user and not try anything smart
 *
 * {@value #ON_FAIL_TRY_ONE_NEXT_AVAILABLE} will try one more random server before returning to the
 * user with an error
 *
 * {@value #ON_FAIL_TRY_ALL_AVAILABLE} will try all available servers in the cluster before giving
 * up and returning the communication error to the user.
 *
 */
public class FailoverPolicy {
  private static final Logger log = LoggerFactory.getLogger(FailoverPolicy.class);

  /** On communication failure, just return the error to the client and don't retry */
  public static final FailoverPolicy FAIL_FAST = new FailoverPolicy(0, 0);
  /** On communication error try one more server before giving up */
  public static final FailoverPolicy ON_FAIL_TRY_ONE_NEXT_AVAILABLE = new FailoverPolicy(1, 0);
  /** On communication error try all known servers before giving up */
  public static final FailoverPolicy ON_FAIL_TRY_ALL_AVAILABLE = new FailoverPolicy(Integer.MAX_VALUE - 1, 0);
  /** On communication error degrade consistency from all to quorum to one and retry one 3 times - only works with thread local consistency policy */
  public static final FailoverPolicy ON_FAIL_DEGRADE_CONSISTENCY = new DegradingFailoverPolicy();

  public final int numRetries;

  public final int sleepBetweenHostsMilli;

  public FailoverPolicy(int numRetries, int sleepBwHostsMilli) {
    this.numRetries = numRetries;
    sleepBetweenHostsMilli = sleepBwHostsMilli;
  }

  public ConsistencyLevel checkConsistency(ConsistencyLevel consistency) {
    return consistency;
  }

  public void handleTimeout(FailoverOperator op, int retries){

  }

  public void handleUnavailable(FailoverOperator op, int retries){

  }

  public void handleTransportError(FailoverOperator op, int retries){

  }

  private static class DegradingFailoverPolicy extends FailoverPolicy {
    private ConsistencyLevel newConsistencyLevel = null;
    private long timeout = 0L;
    private static final int DEFAULT_RESET_TIMEOUT_MILLIS = 10000;

    public DegradingFailoverPolicy() {
      super(5, 0);
    }

    @Override
    public void handleUnavailable(FailoverOperator op, int retries) {
      super.handleUnavailable(op, retries);
      degradeConsistency(op, retries);
    }

    @Override
    public void handleTransportError(FailoverOperator op, int retries) {
      super.handleTransportError(op, retries);
      degradeConsistency(op, retries);
    }

    public void degradeConsistency(FailoverOperator op, int attempt) {
      timeout = System.currentTimeMillis() + DEFAULT_RESET_TIMEOUT_MILLIS;
      ConsistencyLevel current = op.getKeyspace().getConsistencyLevel();
      newConsistencyLevel = (ConsistencyLevel.ALL.equals(current)) ? ConsistencyLevel.QUORUM : ConsistencyLevel.ONE;
      log.warn("failure writing at consistence " + current + " switching to " + newConsistencyLevel + ", attempt: " + attempt + ", timeout: " + timeout);
    }

    @Override
    public ConsistencyLevel checkConsistency(ConsistencyLevel consistency) {
      if(timeout > 0 && System.currentTimeMillis() > timeout){
        log.warn("consistency degrading timout passed, moving back to normal consistency");
        timeout = 0;
        newConsistencyLevel = null;
      }
      return newConsistencyLevel != null ? newConsistencyLevel : super.checkConsistency(consistency);
    }
  }
}
