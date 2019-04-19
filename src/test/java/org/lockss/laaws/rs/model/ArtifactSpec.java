package org.lockss.laaws.rs.model;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.Assertions;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RepoUtil;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

// NOTE: this class is used by TestRestLockssRepository in the
// laaws-repository-service project

/**
 * All the info needed to create and store an Artifact, or to compare
 * with a retrieved Artifact
 */
public class ArtifactSpec implements Comparable<Object> {
  private final static L4JLogger log = L4JLogger.getLogger();

  static boolean AVOID_STREAM_CLOSED_BUG = false;

  protected static int MAX_RANDOM_FILE = 50000;
  protected static int MAX_INCR_FILE = 20000;

  static {
    if (AVOID_STREAM_CLOSED_BUG) {
      // avoid Stream Closed bug by staying under 4096
      MAX_RANDOM_FILE = 4000;
      MAX_INCR_FILE = 4000;
    }
  }

  protected static String COLL1 = "coll1";
  protected static String AUID1 = "auid1";

  protected static StatusLine STATUS_LINE_OK =
      new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

  protected static HttpHeaders HEADERS1 = new HttpHeaders();

  static {
    HEADERS1.set("key1", "val1");
    HEADERS1.set("key2", "val2");
  }

  // Identifying fields used in lookups
  String coll = COLL1;
  String auid = AUID1;
  String url;
  int fixedVer = -1;

  // used for creation and comparison of actual with expected
  boolean toDelete = false;
  boolean toCommit = false;
  StatusLine statLine = STATUS_LINE_OK;
  Map<String, String> headers = RepoUtil.mapFromHttpHeaders(HEADERS1);
  String content;
  InputStream iStream;

  // expected values
  long len = -1;
  int expVer = -1;
  String contentDigest;
  String storageUrl;
  long collectionDate = -1;

  // state
  boolean isCommitted = false;
  boolean isDeleted;
  String artId;

  public enum ArtifactDataStoreOperation {
    COMMIT,
    DELETE
  }

  private List<ArtifactDataStoreOperation> dsops = new ArrayList<>();

  public List<ArtifactDataStoreOperation> getDataStoreOperations() {
    return dsops;
  }

  public ArtifactSpec copy() {
    return ArtifactSpec.forCollAuUrl(coll, auid, url)
        .setStatusLine(getStatusLine())
        .setHeaders(new HashMap<String, String>(getHeaders()))
        .setContent(getContent())
        .setContentLength(len);
  }

  public static ArtifactSpec forCollAuUrl(String coll, String auid, String url) {
    return new ArtifactSpec()
        .setCollection(coll)
        .setAuid(auid)
        .setUrl(url);
  }

  public static ArtifactSpec forCollAuUrlVer(String coll, String auid,
                                        String url, int version) {
    return ArtifactSpec.forCollAuUrl(coll, auid, url).setVersion(version);
  }

  public ArtifactSpec setUrl(String url) {
    this.url = url;
    return this;
  }

  public ArtifactSpec setExpVer(int ver) {
    this.expVer = ver;
    return this;
  }

  public ArtifactSpec setCollection(String coll) {
    this.coll = coll;
    return this;
  }

  public ArtifactSpec setAuid(String auid) {
    this.auid = auid;
    return this;
  }

  public ArtifactSpec setContent(String content) {
    this.content = content;
    return this;
  }

  public ArtifactSpec setVersion(int version) {
    this.fixedVer = version;
    return this;
  }

  public ArtifactSpec setArtifactId(String id) {
    this.artId = id;
    return this;
  }

  public ArtifactSpec setContentLength(long len) {
    this.len = len;
    return this;
  }

  public ArtifactSpec setContentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
    return this;
  }

  public ArtifactSpec setStorageUrl(String storageUrl) {
    this.storageUrl = storageUrl;
    return this;
  }

  public ArtifactSpec setHeaders(Map<String, String> headers) {
    this.headers = headers;
    return this;
  }

  public ArtifactSpec setStatusLine(StatusLine statLine) {
    this.statLine = statLine;
    return this;
  }

  public ArtifactSpec thenCommit() {
    dsops.add(ArtifactDataStoreOperation.COMMIT);
    toCommit(true);
    return this;
  }

  public ArtifactSpec thenDelete() {
    dsops.add(ArtifactDataStoreOperation.DELETE);
    toDelete(true);
    return this;
  }

  public ArtifactSpec toCommit(boolean toCommit) {
    this.toCommit = toCommit;
    return this;
  }

  public ArtifactSpec toDelete(boolean toDelete) {
    this.toDelete = toDelete;
    return this;
  }

  public boolean isToCommit() {
    return toCommit;
  }

  public boolean isToDelete() {
    return toDelete;
  }

  public ArtifactSpec setCommitted(boolean committed) {
    this.isCommitted = committed;
    return this;
  }

  public ArtifactSpec setDeleted(boolean deleted) {
    this.isDeleted = deleted;
    return this;
  }

  public boolean isCommitted() {
    return isCommitted;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public String getUrl() {
    return url;
  }

  public String getCollection() {
    return coll;
  }

  public String getAuid() {
    return auid;
  }

  public int getVersion() {
    return fixedVer;
  }

  public boolean hasVersion() {
    return fixedVer >= 0;
  }

  public int getExpVer() {
    return expVer;
  }

  public String getArtifactId() {
    return artId;
  }

  public boolean hasContent() {
    return content != null || iStream != null;
  }

  public ArtifactSpec generateContent() {
    if (len >= 0) {
      if (len > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Refusing to generate content > 2GB: "
            + len);
      }
      setContent(RandomStringUtils.randomAlphabetic((int) len));
    } else {
      setContent(RandomStringUtils.randomAlphabetic(0, MAX_RANDOM_FILE));
    }
    log.info("gen content");
    return this;
  }

  public String getContent() {
    return content;
  }

  public long getContentLength() {
    if (len >= 0) {
      return len;
    } else if (content != null) {
      return content.length();
    } else {
      throw new IllegalStateException("getContentLen() called when length unknown");
    }
  }

  public String getContentDigest() {
    log.trace("content = " + content);
    // Check whether content has been defined.
    if (content != null) {
      // Yes: Check whether the content digest needs to be computed.
      if (contentDigest == null) {
        // Yes: Compute it.
        String algorithmName = ArtifactData.DEFAULT_DIGEST_ALGORITHM;

        try {
          MessageDigest digest = MessageDigest.getInstance(algorithmName);
          contentDigest = String.format("%s:%s", digest.getAlgorithm(),
              new String(Hex.encodeHex(
                  digest.digest(content.getBytes(StandardCharsets.UTF_8)))));
        } catch (NoSuchAlgorithmException nsae) {
          String errMsg = String.format("Unknown digest algorithm: %s; "
              + "could not instantiate a MessageDigest", algorithmName);
          log.error(errMsg);
          throw new RuntimeException(errMsg);
        }
      }

      log.trace("contentDigest = " + contentDigest);
      return contentDigest;
    } else {
      // No: Report the problem.
      throw new IllegalStateException(
          "getContentDigest() called when content unknown");
    }
  }

  public String getStorageUrl() {
    return storageUrl;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getHeadersAsText() {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, String> entry : headers.entrySet()) {
      sb.append(entry.getKey()).append(": ").append(entry.getValue())
          .append("\n");
    }

    return sb.toString();
  }

  public StatusLine getStatusLine() {
    return statLine;
  }

  public long getCollectionDate() {
    if (collectionDate >= 0) {
      return collectionDate;
    } else if (getArtifactData() != null) {
      return getArtifactData().getCollectionDate();
    } else {
      throw new IllegalStateException("getCollectionDate() called when collection date unknown");
    }
  }

  public HttpHeaders getMetdata() {
    return RepoUtil.httpHeadersFromMap(headers);
  }

  public ArtifactIdentifier getArtifactIdentifier() {
    return new ArtifactIdentifier(artId, coll, auid, url, -1);
  }

  public Artifact getArtifact() {
    return new Artifact(
        getArtifactId(),
        getCollection(),
        getAuid(),
        getUrl(),
        getVersion(),
        isCommitted(),
        getStorageUrl(),
        getContentLength(),
        getContentDigest()
    );
  }

  public ArtifactData getArtifactData() {
    return new ArtifactData(getArtifactIdentifier(), getMetdata(),
        getInputStream(), getStatusLine());
  }

  public InputStream getInputStream() {
    if (content != null) {
      return IOUtils.toInputStream(content, Charset.defaultCharset());
    }
    return null;
  }

  /**
   * Order agrees with repository enumeration order: collection, auid,
   * url, version high-to-low
   */
  public int compareTo(Object o) {
    ArtifactSpec s = (ArtifactSpec) o;
    return new CompareToBuilder()
        .append(this.getCollection(), s.getCollection())
        .append(this.getAuid(), s.getAuid())
        .append(this.getUrl(), s.getUrl())
        .append(s.getVersion(), this.getVersion())
        .toComparison();
  }

  /**
   * Return a key that's unique to the collection,au,url
   */
  public String artButVerKey() {
    return getCollection() + "|" + getAuid() + "|" + getUrl();
  }

  /**
   * true if other refers to an artifact with the same collection, auid
   * and url, independent of version.
   */
  public boolean sameArtButVer(ArtifactSpec other) {
    return artButVerKey().equals(other.artButVerKey());
  }

  /** true if other refers to an artifact with the same collection
   * and url, independent of AU and version. */
  public boolean sameArtButVerAllAus(ArtifactSpec other) {
    return artButVerKeyAllAus().equals(other.artButVerKeyAllAus());
  }

  /** Return a key that's unique to the collection,url */
  public String artButVerKeyAllAus() {
    return getCollection() + "|" + getUrl();
  }

  /**
   * Assert that the Artifact matches this ArtifactSpec
   */
  public void assertArtifact(LockssRepository repository, Artifact art) throws IOException {
    try {
      assertArtifactCommon(art);

      // Test for getArtifactData(Artifact)
      ArtifactData ad1 = repository.getArtifactData(art);
      Assertions.assertEquals(art.getIdentifier(), ad1.getIdentifier());
      Assertions.assertEquals(getContentLength(), ad1.getContentLength());
      Assertions.assertEquals(getContentDigest(), ad1.getContentDigest());
      assertArtifactData(ad1);

      // Test for getArtifactData(String, String)
      ArtifactData ad2 = repository.getArtifactData(getCollection(), art.getId());
      Assertions.assertEquals(getContentLength(), ad2.getContentLength());
      Assertions.assertEquals(getContentDigest(), ad2.getContentDigest());
      assertArtifactData(ad2);
    } catch (Exception e) {
      log.error("Caught exception asserting artifact: {}", e);
      log.error("art = {}", art);
      log.error("spec = {}", this);
      throw e;
    }
  }

  public void assertArtifact(ArtifactDataStore store, Artifact artifact) throws IOException {
    try {
      assertArtifactCommon(artifact);

      // Test for getArtifactData(Artifact)
      ArtifactData ad1 = store.getArtifactData(artifact);
      Assertions.assertEquals(artifact.getIdentifier(), ad1.getIdentifier());
      Assertions.assertEquals(getContentLength(), ad1.getContentLength());
      Assertions.assertEquals(getContentDigest(), ad1.getContentDigest());
      assertArtifactData(ad1);
    } catch (Exception e) {
      log.error( "Caught exception asserting artifact [artifactId: {}, artifactSpec = {}]: {}", artifact, this, e);
      throw e;
    }
  }

  public void assertArtifactCommon(Artifact art) {
    Assertions.assertNotNull(art, "Comparing with " + this);

    Assertions.assertEquals(getCollection(), art.getCollection());
    Assertions.assertEquals(getAuid(), art.getAuid());
    Assertions.assertEquals(getUrl(), art.getUri());

    if (getExpVer() >= 0) {
      Assertions.assertEquals(getExpVer(), (int) art.getVersion());
    }

    Assertions.assertEquals(getContentLength(), art.getContentLength());
    Assertions.assertEquals(getContentDigest(), art.getContentDigest());

    if (getStorageUrl() != null) {
      Assertions.assertEquals(getStorageUrl(), art.getStorageUrl());
    }
  }

  public void assertEquals(StatusLine exp, StatusLine line) {
    Assertions.assertEquals(exp.toString(), line.toString());
  }

  /**
   * Assert that the ArtifactData matches the ArtifactSpec
   */
  public void assertArtifactData(ArtifactData ad) {
    Assertions.assertNotNull(ad, "Didn't find ArticleData for: " + this);
    assertEquals(getStatusLine(), ad.getHttpStatus());
    Assertions.assertEquals(getContentLength(), ad.getContentLength());
    Assertions.assertEquals(getContentDigest(), ad.getContentDigest());

    if (getStorageUrl() != null) {
      Assertions.assertEquals(getStorageUrl(), ad.getStorageUrl());
    }

    new LockssTestCase5().assertSameBytes(getInputStream(), ad.getInputStream(), getContentLength());
    Assertions.assertEquals(getHeaders(), RepoUtil.mapFromHttpHeaders(ad.getMetadata()));
  }

  /**
   * Assert that the sequence of Artifacts matches the stream of ArtifactSpecs
   */
  public static void assertArtList(LockssRepository repository,
                                   Stream<ArtifactSpec> expSpecs, Iterable<Artifact> arts) throws IOException {
    Iterator<ArtifactSpec> specIter = expSpecs.iterator();
    Iterator<Artifact> artIter = arts.iterator();
    while (specIter.hasNext() && artIter.hasNext()) {
      ArtifactSpec spec = specIter.next();
      Artifact art = artIter.next();
      spec.assertArtifact(repository, art);
    }
    Assertions.assertFalse(specIter.hasNext());
    Assertions.assertFalse(artIter.hasNext());
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("[ArtifactSpec: (%s,%s,%s,%d)", url, coll, auid, fixedVer));
    if (isCommitted()) {
      sb.append("C");
    }
    if (hasContent()) {
      sb.append(String.format(", len: %s", getContentLength()));
// 	sb.append(String.format(", len: %s, content: %.30s",
// 				getContentLength(), getContent()));
    }
    sb.append("]");
    return sb.toString();
  }
}