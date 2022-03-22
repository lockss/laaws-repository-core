package org.lockss.laaws.rs.util;

import org.lockss.laaws.rs.io.index.solr.SolrArtifact;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArtifactUtil {
  /** Return a String that uniquely identifies this Artifact */
  public static String makeKey(Artifact artifact) {
    return makeKey(artifact.getCollection(), artifact.getAuid(),
        artifact.getUri(), artifact.getVersion());
  }

  /** Return a String that uniquely identifies the Artifact with the
   * specified values.  version -1 means latest version */
  public static String makeKey(String collection, String auid, String uri, int version) {
    StringBuilder sb = new StringBuilder(200);
    sb.append(collection);
    sb.append(":");
    sb.append(auid);
    sb.append(":");
    sb.append(uri);
    sb.append(":");
    sb.append(version);
    return sb.toString();
  }

  /** Return a String that uniquely identifies "the latest committed
   * version of the Artifact" */
  public static String makeLatestKey(Artifact artifact) {
    return makeLatestKey(artifact.getCollection(), artifact.getAuid(), artifact.getUri());
  }

  /** Return a String that uniquely identifies "the latest committed
   * version of the Artifact with the specified values" */
  public static String makeLatestKey(String collection, String auid,
                                     String uri) {
    return makeKey(collection, auid, uri, -1);
  }

  // matches ":<ver>" at the end
  static Pattern LATEST_VER_PATTERN = Pattern.compile(":[^:]+$");

  /** Return a String that uniquely identifies "the latest committed
   * version of the Artifact with the specified key" */
  public static String makeLatestKey(String key) {
    Matcher m1 = LATEST_VER_PATTERN.matcher(key);
    if (m1.find()) {
      return m1.replaceAll(":-1");
    }
    return null;
  }

  public static Artifact from(ArtifactData artifactData) {
    ArtifactIdentifier artifactId = ArtifactIdentifierUtil.from(artifactData);

    return new Artifact()
        .id(artifactId.getId())
        .collection(artifactId.getCollection())
        .auid(artifactId.getAuid())
        .uri(artifactId.getUri())
        .version(artifactId.getVersion())
        .committed(artifactData.getCommitted())
        .storageUrl(artifactData.getStorageUrl())
        .contentLength(artifactData.getContentLength())
        .contentDigest(artifactData.getContentDigest())
        .collectionDate(artifactData.getCollectionDate());
  }

  public static Artifact copyOf(Artifact artifactRef) {
    return new Artifact()
        .id(artifactRef.getId())
        .collection(artifactRef.getCollection())
        .auid(artifactRef.getAuid())
        .uri(artifactRef.getUri())
        .version(artifactRef.getVersion())
        .committed(artifactRef.getCommitted())
        .storageUrl(artifactRef.getStorageUrl())
        .contentLength(artifactRef.getContentLength())
        .contentDigest(artifactRef.getContentDigest())
        .contentType(artifactRef.getContentType())
        .collectionDate(artifactRef.getCollectionDate())
        .properties(artifactRef.getProperties());

  }

  public static Artifact from(SolrArtifact solrArtifact) {
    Artifact a = new Artifact();

    a.setId(solrArtifact.getId());
    a.setCollection(solrArtifact.getCollection());
    a.setAuid(solrArtifact.getAuid());
    a.setUri(solrArtifact.getUri());
    a.setVersion(solrArtifact.getVersion());

    a.setCommitted(solrArtifact.getCommitted());
    a.setStorageUrl(solrArtifact.getStorageUrl());

    a.setContentLength(solrArtifact.getContentLength());
    a.setContentDigest(solrArtifact.getContentDigest());
    a.setContentType(solrArtifact.getContentType());

    a.setCollectionDate(solrArtifact.getCollectionDate());

    a.setProperties(solrArtifact.getProperties());

    return a;
  }
}
