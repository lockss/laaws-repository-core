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
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  // state
  boolean isCommitted = false;
  String artId;

  public static <WADS extends WarcArtifactDataStore> void populateDataStore(WADS store, List<ArtifactSpec> artifactSpecs) {
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

  public ArtifactSpec toCommit(boolean toCommit) {
    this.toCommit = toCommit;
    return this;
  }

  public boolean isToCommit() {
    return toCommit;
  }

  public ArtifactSpec setCommitted(boolean committed) {
    this.isCommitted = committed;
    return this;
  }

  public boolean isCommitted() {
    return isCommitted;
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

  public HttpHeaders getMetdata() {
    return RepoUtil.httpHeadersFromMap(headers);
  }

  public ArtifactIdentifier getArtifactIdentifier() {
    return new ArtifactIdentifier(coll, auid, url, -1);
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

  /**
   * Assert that the Artifact matches this ArtifactSpec
   */
  public void assertData(LockssRepository repository, Artifact art)
      throws IOException {
    try {
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

      ArtifactData ad = repository.getArtifactData(art);
      Assertions.assertEquals(art.getIdentifier(), ad.getIdentifier());
      Assertions.assertEquals(getContentLength(), ad.getContentLength());
      Assertions.assertEquals(getContentDigest(), ad.getContentDigest());
      assertData(repository, ad);

      ArtifactData ad2 = repository.getArtifactData(getCollection(),
          art.getId());
      Assertions.assertEquals(getContentLength(), ad2.getContentLength());
      Assertions.assertEquals(getContentDigest(), ad2.getContentDigest());
      assertData(repository, ad2);
    } catch (Exception e) {
      log.error("Caught exception asserting artifact: {}", e);
      log.error("art = {}", art);
      log.error("spec = {}", this);
      throw e;
    }
  }

  public void assertEquals(StatusLine exp, StatusLine line) {
    Assertions.assertEquals(exp.toString(), line.toString());
  }

  /**
   * Assert that the ArtifactData matches the ArtifactSpec
   */
  public void assertData(LockssRepository repository, ArtifactData ad)
      throws IOException {
    Assertions.assertNotNull(ad, "Didn't find ArticleData for: " + this);
    assertEquals(getStatusLine(), ad.getHttpStatus());
    Assertions.assertEquals(getContentLength(), ad.getContentLength());
    Assertions.assertEquals(getContentDigest(), ad.getContentDigest());

    if (getStorageUrl() != null) {
      Assertions.assertEquals(getStorageUrl(), ad.getStorageUrl());
    }

    new LockssTestCase5().assertSameBytes(getInputStream(),
        ad.getInputStream(), getContentLength());
    Assertions.assertEquals(getHeaders(),
        RepoUtil.mapFromHttpHeaders(ad.getMetadata()));
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
      spec.assertData(repository, art);
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