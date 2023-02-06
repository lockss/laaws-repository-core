package org.lockss.laaws.rs.core;

public interface LockssRepositorySubsystem {
  void setLockssRepository(BaseLockssRepository repository);
  void init();
  void start();
  void stop();
}
