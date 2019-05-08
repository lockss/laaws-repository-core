package org.lockss.laaws.rs.io;

import org.apache.commons.lang3.tuple.Pair;

public class RepoAuid extends Pair<String, String> {
  private String collection;
  private String auid;

  public RepoAuid(String collection, String auid) {
    this.collection = collection;
    this.auid = auid;
  }

  @Override
  public String getLeft() {
    return collection;
  }

  @Override
  public String getRight() {
    return auid;
  }

  @Override
  public String setValue(String auid) {
    this.auid = auid;
    return auid;
  }
}
