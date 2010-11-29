package me.prettyprint.cassandra.service;

/**
 * Resolution used to create timestamps.
 * Clients may wish to use millisec, micro or sec, depending on the application
 * needs and existing data and other tools, so Hector makes that
 * configurable.
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public enum TimestampResolution {
  SECONDS, MILLISECONDS, MICROSECONDS;

  private static final long ONE_THOUSAND = 1000L;

  /** The last time value issued. Used to try to prevent duplicates. */
  private static long lastTime = -1;

  static {
    synchronized (TimestampResolution.class) {
      lastTime = System.currentTimeMillis() * ONE_THOUSAND;
    }
  }

  public long createTimestamp() {
    switch (this) {
      case MICROSECONDS:
        // The following simmulates a microsec resolution by advancing a static counter every time
        // a client calls the createClock method, simulating a tick.
        long us = System.currentTimeMillis() * ONE_THOUSAND;
        synchronized (TimestampResolution.this) {
          if (us > lastTime) {
            lastTime = us;
          } else {
            // the time i got from the system is equals or less (hope not - clock going backwards)
            // One more "microsecond"
            us = ++lastTime;
          }
        }
        return us;
      case MILLISECONDS:
        return System.currentTimeMillis();
      case SECONDS:
        return System.currentTimeMillis() / ONE_THOUSAND;
    }

    return System.currentTimeMillis();
  }
}
