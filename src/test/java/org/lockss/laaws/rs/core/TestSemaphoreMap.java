package org.lockss.laaws.rs.core;

import org.junit.Test;
import org.lockss.util.test.LockssTestCase5;

public class TestSemaphoreMap extends LockssTestCase5 {

  private static final String TEST_KEY = "key";

  @Test
  public void testSemaphoreMap() throws Exception {
    SemaphoreMap<String> locks = new SemaphoreMap<>();

    // Assert usage count is zero if key is not in map
    assertEquals(0, locks.getCount(TEST_KEY));

    // Release lock and assert usage count is still zero
    locks.releaseLock(TEST_KEY);
    assertEquals(0, locks.getCount(TEST_KEY));

    // Acquire lock and assert it is incremented to one
    locks.getLock(TEST_KEY);
    assertEquals(1, locks.getCount(TEST_KEY));

    // Release lock and assert it is decremented to zero
    locks.releaseLock(TEST_KEY);
    assertEquals(0, locks.getCount(TEST_KEY));
  }

}
