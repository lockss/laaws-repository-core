package org.lockss.laaws.rs.io;

import java.util.concurrent.locks.ReentrantLock;

public class WarcFileLockMap extends LockMap<String> {
  public WarcFileLockMap() {
    super(ReentrantLock::new);
  }
}
