package org.lockss.laaws.rs.util;

import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;

public class ArtifactIdentifierUtil {
  public static ArtifactIdentifier from(Artifact artifact) {
    return new ArtifactIdentifier()
        .id(artifact.getId())
        .collection(artifact.getCollection())
        .auid(artifact.getAuid())
        .uri(artifact.getUri())
        .version(artifact.getVersion());
  }

  public static ArtifactIdentifier from(ArtifactData ad) {
    return new ArtifactIdentifier()
        .id(ad.getId())
        .collection(ad.getCollection())
        .auid(ad.getAuid())
        .uri(ad.getUri())
        .version(ad.getVersion());
  }
}
