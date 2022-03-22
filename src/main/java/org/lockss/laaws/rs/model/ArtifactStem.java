package org.lockss.laaws.rs.model;

import java.util.Objects;

/**
 * Struct representing a tuple of collection ID, AUID, and URL. Used for artifact version locking.
 *
 * FIXME: Define this in OAS?
 */
public class ArtifactStem {
  private final String collection;
  private final String auid;
  private final String uri;

  public ArtifactStem(ArtifactIdentifier id) {
    this(id.getCollection(), id.getAuid(), id.getUri());
  }

  public ArtifactStem(String collection, String auid, String uri) {
    this.collection = collection;
    this.auid = auid;
    this.uri = uri;
  }

  public static ArtifactStem from(ArtifactData ad) {
    return new ArtifactStem(ad.getCollection(), ad.getAuid(), ad.getUri());
  }

  public static ArtifactStem from(Artifact artifact) {
    return new ArtifactStem(artifact.getCollection(), artifact.getAuid(), artifact.getUri());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArtifactStem that = (ArtifactStem) o;
    return collection.equals(that.collection) && auid.equals(that.auid) && uri.equals(that.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, auid, uri);
  }
}
