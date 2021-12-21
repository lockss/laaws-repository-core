package org.lockss.laaws.rs.core;

public interface LockssRepositorySubsystem {
  void setLockssRepository(LockssRepository repository);
  void init();
  void start();
  void stop();
}
