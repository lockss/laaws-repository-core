package org.lockss.laaws.rs.core;

import org.junit.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestSemaphoreMap extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static String TEST_KEY = "test";
  private final static long MAX_THREADS = 10;

  @Test
  public void testSemaphoreMap() throws Exception {
    SemaphoreMap<String> locks = new SemaphoreMap<>();

    // Assert usage count is null if key is not in map
    assertNull(locks.getCount(TEST_KEY));

    // Release lock and assert usage count is still zero (will generate a warning message)
    locks.releaseLock(TEST_KEY);
    assertNull(locks.getCount(TEST_KEY));

    // Acquire lock and assert it is incremented to one
    locks.getLock(TEST_KEY);
    assertEquals(1, locks.getCount(TEST_KEY));

    // Release lock and assert it is decremented to zero
    locks.releaseLock(TEST_KEY);
    assertNull(locks.getCount(TEST_KEY));
  }

  @Test
  public void testThreadAcquireRelease() throws Exception {
    SemaphoreMap<String> locks = new SemaphoreMap<>();

    // Start threads
    for (int i = 0; i < MAX_THREADS; i++) {
      Thread t = new Thread(new SemaphoreMapTestRunnable(locks));
      t.start();
    }

    // Assert only one thread acquires the semaphore at a time
    for (int i = 0; i < 3*MAX_THREADS; i++) {
      assertEquals(0, queue.take());
    }

    // Assert that the semaphores were deleted from the map
    assertEquals(0, locks.getSize());
  }

  private int counter = 0;
  private BlockingQueue queue = new ArrayBlockingQueue((int) (4*MAX_THREADS));

  private class SemaphoreMapTestRunnable implements Runnable {
    private SemaphoreMap locks;

    public SemaphoreMapTestRunnable(SemaphoreMap locks) {
      this.locks = locks;
    }

    @Override
    public void run() {
      try {
        locks.getLock(TEST_KEY);
        queue.offer(counter++);
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        queue.offer(--counter);
        locks.releaseLock(TEST_KEY);
        queue.offer(0); // good enough for test code
      }
    }
  }

}
