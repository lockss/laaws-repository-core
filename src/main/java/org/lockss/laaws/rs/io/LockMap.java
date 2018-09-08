package org.lockss.laaws.rs.io;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

public class LockMap<K> {
  ConcurrentHashMap<K, Lock> lockMap = new ConcurrentHashMap<>();
  Supplier<Lock> lockSupplier;

  public LockMap(Supplier<Lock> lockSup) {
    lockSupplier = lockSup;
  }

  public Lock getLock(K key) {
    Lock x = lockSupplier.get();
    Lock y = lockMap.putIfAbsent(key, x);
    return (y == null) ? x : y;
  }
}
