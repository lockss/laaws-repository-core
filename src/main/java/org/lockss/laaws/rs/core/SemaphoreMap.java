package org.lockss.laaws.rs.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Maintains a semaphore per key for the duration it is needed.
 *
 * @param <T> The class type of the keys.
 */
public class SemaphoreMap<T> {

  /**
   * Map from key to semaphore and its usage count.
   */
  private Map<T, SemaphoreAndCount> locks = new HashMap<>();

  /**
   * Internal struct containing a semaphore and its usage count.
   */
  private static class SemaphoreAndCount {
    private Semaphore sm;
    private long count;

    /**
     * Constructor.
     */
    public SemaphoreAndCount() {
      this.sm = new Semaphore(1);
      this.count = 0;
    }

    /**
     * Returns the semaphore.
     *
     * @return The {@link Semaphore}.
     */
    public Semaphore getSemaphore() {
      return sm;
    }

    /**
     * Increments the usage counter of this semaphore.
     */
    public void incrementCounter() {
      count++;
    }

    /**
     * Decrements the usage counter of this semaphore and returns it.
     *
     * @return A {@code long} containing the usage count.
     */
    public long decrementCounter() {
      return --count;
    }
  }

  /**
   * Returns the existing internal {@link SemaphoreAndCount} of a key, or creates and returns a new one.
   *
   * @param key The key of the {@link SemaphoreAndCount} to return.
   * @return The {@link SemaphoreAndCount} of the key.
   */
  private SemaphoreAndCount getSemaphoreAndCount(T key) {
    // Get semaphore and count from internal map
    SemaphoreAndCount snc = locks.get(key);

    // Create a new semaphore and count if one did not exist in the map
    if (snc == null) {
      snc = new SemaphoreAndCount();
      locks.put(key, snc);
    }

    return snc;
  }

  /**
   * Acquires the lock (or blocks until it can be acquired) for the semaphore of the provided key.
   *
   * @param key The key of the semaphore to acquire the lock of.
   * @throws InterruptedException Thrown if the thread is interrupted while waiting to acquire.
   */
  public void getLock(T key) throws InterruptedException {
    SemaphoreAndCount snc;

    // Get the semaphore and increase its usage count
    synchronized (locks) {
      snc = getSemaphoreAndCount(key);
      snc.incrementCounter();
    }

    // May block until it can be acquired
    snc.getSemaphore().acquire();
  }

  /**
   * Releases the lock for the semaphore of the provided key.
   *
   * @param key The key of the semaphore to release the lock of.
   */
  public void releaseLock(T key) {
    synchronized (locks) {
      // Release the semaphore lock
      SemaphoreAndCount snc = getSemaphoreAndCount(key);
      snc.getSemaphore().release();

      // Decrement the usage count; remove the semaphore from map if no longer in use
      if (snc.decrementCounter() < 1) {
        locks.remove(key);
      }
    }
  }

  /**
   * Returns an estimate of the usage count of a semaphore. Only intended to be used in testing.
   *
   * @param key The key of the semaphore.
   * @return A {@code long} containing the usage count of the semaphore.
   */
  public long getCount(T key) {
    SemaphoreAndCount snc = locks.get(key);
    return (snc == null) ? 0 : snc.count;
  }
}
