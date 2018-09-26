package org.lockss.laaws.rs.io;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.locks.ReentrantLock;

public class AuidLockMap extends LockMap<Pair<String,String>> {
  public AuidLockMap() {
    super(ReentrantLock::new);
  }
}
