package org.lockss.laaws.rs.io.storage.warc;

import org.lockss.laaws.rs.model.Artifact;

/**
 * States of {@link Artifact} objects within a {@link WarcArtifactDataStore}.
  */
public enum ArtifactState {
  UNKNOWN,
  NOT_INDEXED,
  UNCOMMITTED,
  COMMITTED,
  COPIED,
  EXPIRED,
  DELETED
}