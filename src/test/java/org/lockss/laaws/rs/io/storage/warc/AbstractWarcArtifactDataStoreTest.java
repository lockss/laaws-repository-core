/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.io.storage.warc;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.EnumSource;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore.ArtifactState;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.util.test.VariantTest;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeUtil;
import org.mockito.ArgumentMatchers;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.mockito.Mockito.*;

public abstract class AbstractWarcArtifactDataStoreTest<WADS extends WarcArtifactDataStore> extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  // Handle to the data store under test
  protected WADS store;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @BeforeEach
  public void setupTestContext() throws Exception {
    TimeBase.setSimulated();

    // Create a volatile index for all data store tests
    ArtifactIndex index = new VolatileArtifactIndex();
    index.initIndex();

    // Create a new WARC artifact data store
    store = makeWarcArtifactDataStore(index);
    assertNotNull(store.getArtifactIndex());
    assertSame(index, store.getArtifactIndex());

    // Initialize data store and assert state
    store.initDataStore();
    assertEquals(WarcArtifactDataStore.DataStoreState.INITIALIZED, store.getDataStoreState());

    // Setup variant
    beforeVariant();
  }

  @AfterEach
  public void teardownDataStore() throws InterruptedException {
    ArtifactIndex index = store.getArtifactIndex();
    store.shutdownDataStore();

    if (index != null) {
      index.shutdownIndex();
    }
  }

  // *******************************************************************************************************************
  // * VARIANT FRAMEWORK
  // *******************************************************************************************************************

  // ArtifactSpec for each Artifact that has been added to the repository
  protected VariantState variantState = new VariantState();
  protected String variant = "no_variant";

  public enum TestRepoScenarios {
    empty, commit1, delete1, double_delete, double_commit, commit_delete_2x2, overlap
  }

  // Commonly used artifact identifiers and contents
  protected static String COLL1 = "coll1";
  protected static String COLL2 = "coll2";
  protected static String AUID1 = "auid1";
  protected static String AUID2 = "auid2";
  protected static String ARTID1 = "art_id_1";

  protected static String URL1 = "http://host1.com/path";
  protected static String URL2 = "http://host2.com/file1";

  // Identifiers expected not to exist in the repository
  protected static String NO_COLL = "no_coll";
  protected static String NO_AUID = "no_auid";
  protected static String NO_URL = "no_url";
  protected static String NO_ARTID = "not an artifact ID";

  // These describe artifacts that getArtifact() should never find
  protected ArtifactSpec[] neverFoundArtifactSpecs = {
      ArtifactSpec.forCollAuUrl(NO_COLL, AUID1, URL1),
      ArtifactSpec.forCollAuUrl(COLL1, NO_AUID, URL1),
      ArtifactSpec.forCollAuUrl(COLL1, AUID1, NO_URL),
  };

  protected List<ArtifactSpec> getArtifactSpecsForVariant(String variant) {
    List<ArtifactSpec> specs = new ArrayList<>();

    switch (variant) {
      case "no_variant":
        // Not a variant test
        break;

      case "empty":
        // Empty data store
        break;

      case "commit1":
        // One committed artifact
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenCommit());
        break;

      case "delete1":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenDelete());
        break;

      case "double_delete":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenDelete().thenDelete());
        break;

      case "double_commit":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenCommit().thenCommit());
        break;

      case "commit_delete_2x2":
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenDelete().thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl("c", "a", "u").thenCommit().thenDelete());
        break;

      case "overlap":
        // Same URLs in different collections and AUs
        specs.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1));
        specs.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2));
        specs.add(ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL2).thenCommit());

        specs.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL1).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL1).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL1));
        specs.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2).thenCommit());
        specs.add(ArtifactSpec.forCollAuUrl(COLL2, AUID2, URL2));
        break;
    }

    return specs;
  }

  // Invoked automatically before each test by the @VariantTest mechanism
  @Override
  protected void setUpVariant(String variantName) {
    log.info("setUpVariant: " + variantName);
    variant = variantName;
  }

  protected void beforeVariant() throws Exception {
    // Populate
    List<ArtifactSpec> artifactSpecs = getArtifactSpecsForVariant(variant);

    log.debug("variant: {}, artifactSpecs.size() = {}", variant, artifactSpecs.size());

    populateDataStore(store, artifactSpecs);
  }

  protected void populateDataStore(WADS store, List<ArtifactSpec> artifactSpecs) throws Exception {
    log.debug("artifactSpecs.size() = {}", artifactSpecs.size());

    for (ArtifactSpec spec : artifactSpecs) {
      Artifact artifact = addArtifactData(spec);

      // Replay data store operations on this artifact data
      for (ArtifactSpec.ArtifactDataStoreOperation op : spec.getDataStoreOperations()) {

        switch (op) {
          case COMMIT:
            commitArtifactData(spec, artifact);
            break;

          case DELETE:
            deleteArtifactData(spec, artifact);
            assertTrue(spec.isDeleted());
            break;

          default:
            log.error("Unknown data store operation in artifact specification [op: {}, spec: {}]", op, spec);
            throw new IllegalStateException("Unknown data store operation in artifact specification");
        }
      }

      variantState.add(spec);
    }

  }

  /**
   * Asserts that data store
   *
   * @throws IOException
   */
  private void assertVariantState() throws IOException, URISyntaxException {
    // Get a handle to the data store's index
    ArtifactIndex index = store.getArtifactIndex();
    assertNotNull(index);

    // Assert variant state against data store
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (spec.isToDelete()) {
        // Assert that the variant framework deleted the artifact
        assertTrue(spec.isDeleted());

        // Assert retrieving this artifact from the data store and index returns null
        assertNull(index.getArtifact(spec.getArtifactId()));
        assertNull(store.getArtifactData(spec.getArtifact()));

        // Data store should have recorded into the repository metadata journal that this artifact is removed
        assertTrue(store.isArtifactDeleted(spec.getArtifactIdentifier()));
      } else {
        // Assert that the variant framework did NOT delete this artifact
        assertFalse(spec.isDeleted());

        // Assert that we get back the expected artifact and artifact data from index and data store, respectively
        assertTrue(index.artifactExists(spec.getArtifactId()));
        Artifact indexedArtifact = index.getArtifact(spec.getArtifactId());
        ArtifactData storedArtifactData = store.getArtifactData(spec.getArtifact());
        spec.assertArtifact(store, indexedArtifact);
        spec.assertArtifactData(storedArtifactData);

        // Assert the artifact is not marked deleted in the repository metadata journal
        assertFalse(store.isArtifactDeleted(spec.getArtifactIdentifier()));

        // Get artifact's storage URL in data store
        String storageUrl = indexedArtifact.getStorageUrl();
        URI tmpWarcBaseStorageUrl = store.makeStorageUrl(store.getTmpWarcBasePaths()[0], null);

        // Assert committed status of artifacts
        if (spec.isToCommit()) {
          // YES: Assert variant framework committed this artifact (sanity check)
          assertTrue(spec.isCommitted());

          // Assert artifact is marked committed in data store's repository metadata journal and index
          assertTrue(store.isArtifactCommitted(spec.getArtifactIdentifier()));
          assertTrue(indexedArtifact.getCommitted());

          // Assert artifact does NOT reside in temporary storage
          assertFalse(store.isTmpStorage(WarcArtifactDataStore.getPathFromStorageUrl(new URI(storageUrl))));
        } else {
          // NO: Assert variant framework DID NOT commit this artifact (sanity check)
          assertFalse(spec.isCommitted());

          // Assert artifact is NOT marked committed in data store's repository metadata journal and index
          assertFalse(store.isArtifactCommitted(spec.getArtifactIdentifier()));
          assertFalse(indexedArtifact.getCommitted());

          // Assert artifact does resides in temporary storage
          assertTrue(storageUrl.startsWith(tmpWarcBaseStorageUrl.toString()));
        }
      }
    }
  }

  protected void deleteArtifactData(ArtifactSpec spec, Artifact artifact) throws IOException {
    String artifactId = spec.getArtifactId();

    log.debug("Deleting artifact from data store [artifactId: {}]", artifactId);

    // Sanity check
//    assertEquals(artifactId, artifact.getId());
//    assertTrue(spec.isToDelete());

    store.deleteArtifactData(artifact);

    // Assert data store repository metadata journal
    assertTrue(store.isArtifactDeleted(artifact.getIdentifier()));

    // Assert index state
    ArtifactIndex index = store.getArtifactIndex();
    assertNotNull(index);
    assertNull(index.getArtifact(artifactId));

    // Update the deleted state in artifact specification
    spec.setDeleted(true);

    // Logging
    log.debug("spec.isDeleted() = {}", spec.isDeleted());
  }

  protected void commitArtifactData(ArtifactSpec spec, Artifact artifact) throws IOException {
    String artifactId = artifact.getId();

    log.debug("Committing artifact [artifactId: {}]", artifactId);

    // Sanity check
//    assertTrue(spec.isToCommit());
//    assertEquals(spec.getArtifactId(), artifactId);

    // Assert current state matches spec
    assertEquals(spec.isCommitted(), artifact.getCommitted());
    assertEquals(spec.isCommitted(), store.isArtifactCommitted(artifact.getIdentifier()));

    // Get a handle to the data store's index
    ArtifactIndex index = store.getArtifactIndex();
    assertNotNull(index);

    if (spec.isDeleted()) {
      // Yes: Assert spec's artifact is deleted from the index
      assertFalse(index.artifactExists(artifactId));
      return;
    }

    // Get the indexed Artifact
    assertTrue(index.artifactExists(artifactId));
    Artifact indexed = index.getArtifact(artifactId);
    assertNotNull(indexed);

    // Sanity check: Assert current committed state matches
    assertEquals(spec.isCommitted(), indexed.getCommitted());

    log.trace("artifact.getCommitted = {}", artifact.getCommitted());

    // Mark the artifact as committed in index
    index.commitArtifact(artifactId);

    // Assert committed status in index
    indexed = index.getArtifact(artifactId);
    assertNotNull(indexed);
    assertTrue(indexed.getCommitted());

    log.trace("artifact.getCommitted = {}", artifact.getCommitted());

    // Commit the artifact data in the data store
    Future<Artifact> future = store.commitArtifactData(artifact);
    assertNotNull(future);

    Artifact committedArtifact = null;

    try {
      committedArtifact = future.get(10, TimeUnit.SECONDS); // FIXME: Use other timeout facilities
      assertNotNull(committedArtifact);
    } catch (Exception e) {
      log.error("Caught exception committing artifact: {}", e);
      log.error("spec = {}", spec);
      log.error("artifact = {}", artifact);
      throw new IOException(e); // FIXME: Implement better exception handling
    }

    // Assert things about the committed artifact
    assertTrue(committedArtifact.getCommitted());
    assertTrue(store.isArtifactCommitted(artifact.getIdentifier()));

    // Update artifact specification
    spec.setCommitted(true);
    spec.setStorageUrl(URI.create(committedArtifact.getStorageUrl()));
    spec.assertArtifact(store, committedArtifact);

    log.debug("End commitTestArtifactData");
  }

  private Artifact addArtifactData(ArtifactSpec spec) throws Exception {
    if (!spec.hasContent()) {
      spec.generateContent();
    }

    spec.setArtifactId(UUID.randomUUID().toString());

    log.debug("Adding artifact data from specification: {}", spec);

    // Get an ArtifactData from the artifact specification
    ArtifactData ad = spec.getArtifactData();
    assertNotNull(ad);

    // Add the ArtifactData
    Artifact artifact = store.addArtifactData(ad);
    assertNotNull(artifact);

    // Assert things about the returned Artifact compared to the spec
    try {
      spec.assertArtifact(store, artifact);
    } catch (Exception e) {
      log.error("Caught exception adding uncommitted artifact data: {}", e);
      log.error("spec = {}", spec);
      log.error("ad = {}", ad);
      log.error("artifact = {}", artifact);
      throw e;
    }

    // Assert uncommitted status of the artifact
    assertFalse(artifact.getCommitted());
    assertFalse(store.isArtifactCommitted(artifact.getIdentifier()));

    // Update the artifact specification from resulting artifact
    spec.setArtifactId(artifact.getId());
    spec.setVersion(artifact.getVersion());
    spec.setStorageUrl(URI.create(artifact.getStorageUrl()));

//    assertEquals(ai.getId(), artifact.getId());
//    assertEquals(ai, artifact.getIdentifier());

    // Assert temporary WARC directory exists
//    assertNotNull(store.getTmpWarcBasePath());
//    assertNotNull(store.getAbsolutePath(store.getTmpWarcBasePath()));
//    assertTrue(isDirectory(store.getTmpWarcBasePath()));

    // Assert things about the added artifact's storage URL
    String storageUrl = artifact.getStorageUrl();
    assertNotNull(storageUrl);
    assertFalse(storageUrl.isEmpty());

    // Get the path within the data store
    Path artifactWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(new URI(storageUrl));
    log.debug("storageUrl = {}", storageUrl);
    log.debug("artifactWarcPath = {}", artifactWarcPath);

    // Assert storage
    assertTrue(isFile(artifactWarcPath));
    assertTrue(store.isTmpStorage(artifactWarcPath));

    // Assert things about the index
    ArtifactIndex index = store.getArtifactIndex();
    assertNotNull(index);

    assertTrue(index.artifactExists(artifact.getId()));

    return artifact;
  }

  // *******************************************************************************************************************
  // * ABSTRACT METHODS
  // *******************************************************************************************************************

  protected abstract WADS makeWarcArtifactDataStore(ArtifactIndex index) throws IOException;
  protected abstract WADS makeWarcArtifactDataStore(ArtifactIndex index, WADS otherStore) throws IOException;

  //  protected abstract URI expected_makeStorageUrl(ArtifactIdentifier aid, long offset, long length) throws Exception;
  protected abstract Path[] expected_getBasePaths() throws Exception;
  protected abstract Path[] expected_getTmpWarcBasePaths() throws Exception;

  public abstract void testInitDataStoreImpl() throws Exception;
  public abstract void testInitCollectionImpl() throws Exception;
  public abstract void testInitAuImpl() throws Exception;

  protected abstract boolean pathExists(Path path) throws IOException;
  protected abstract boolean isDirectory(Path path) throws IOException;
  protected abstract boolean isFile(Path path) throws IOException;

  // *******************************************************************************************************************
  // * UTILITY METHODS FOR TESTS
  // *******************************************************************************************************************

  private ArtifactData generateTestArtifactData(String collection, String auid, String uri, int version, long length) throws IOException {
    // HTTP status (200 OK) for use volatile ArtifactData's we'll add to the repository
    StatusLine statusOK = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");

    // Create an artifact and add it to the data store
    ArtifactIdentifier id = new ArtifactIdentifier(UUID.randomUUID().toString(), collection, auid, uri, version);

    // Generate this artifact's data
    try (ByteArrayOutputStream baos1 = new ByteArrayOutputStream((int) length)) {
      byte[] content = new byte[(int) length];
//    Arrays.fill(content, (byte) 0);
      new Random().nextBytes(content);
      baos1.write(content);

      ArtifactData ad = new ArtifactData(id, null, baos1.toInputStream(), statusOK);

      return ad;
    }
  }

  private Iterable<Path> findWarcs(Path[] paths) throws IOException {
    List<Path> warcs = new ArrayList();
    for (Path path : paths) {
      warcs.addAll(store.findWarcs(path));
    }
    return warcs;
  }

  // *******************************************************************************************************************
  // * TESTS: STATIC
  // *******************************************************************************************************************

  @Test
  public void testStaticCode() throws Exception {
    // Assert expected CRLF encoding
    assertArrayEquals("\r\n".getBytes("UTF-8"), WarcArtifactDataStore.CRLF_BYTES);
  }

  // *******************************************************************************************************************
  // * TESTS: ABSTRACT METHODS (ArtifactDataStore)
  // *******************************************************************************************************************

  @Test
  public void testInitCollection() throws Exception {
    testInitCollectionImpl();
  }

  @Test
  public void testInitAu() throws Exception {
    testInitAuImpl();
  }

  // *******************************************************************************************************************
  // * TESTS: ABSTRACT METHODS (from WarcArtifactDataStore)
  // *******************************************************************************************************************

  @Test
  public void testMakeStorageUrl() throws Exception {
    testMakeStorageUrlImpl();
  }

  public abstract void testMakeStorageUrlImpl() throws Exception;

//  @Test
//  public void testGetInputStreamAndSeek() throws Exception {
//    testGetInputStreamAndSeekImpl();
//  }
//  public abstract void testGetInputStreamAndSeekImpl() throws Exception;

//  @Test
//  public void testGetAppendableOutputStream() throws Exception {
//    testGetAppendableOutputStreamImpl();
//  }
//  public abstract void testGetAppendableOutputStreamImpl() throws Exception;

  @Test
  public void testInitWarc() throws Exception {
    testInitWarcImpl();
  }

  public abstract void testInitWarcImpl() throws Exception;

  @Test
  public void testGetWarcLength() throws Exception {
    testGetWarcLengthImpl();
  }

  public abstract void testGetWarcLengthImpl() throws Exception;

  @Test
  public void testFindWarcs() throws Exception {
    testFindWarcsImpl();
  }

  public abstract void testFindWarcsImpl() throws Exception;

  @Test
  public void testRemoveWarc() throws Exception {
    testRemoveWarcImpl();
  }

  public abstract void testRemoveWarcImpl() throws Exception;

  @Test
  public void testGetBlockSize() throws Exception {
    testGetBlockSizeImpl();
  }

  public abstract void testGetBlockSizeImpl() throws Exception;

  @Test
  public void testGetFreeSpace() throws Exception {
    testGetFreeSpaceImpl();
  }

  public abstract void testGetFreeSpaceImpl() throws Exception;


  // *******************************************************************************************************************
  // * TESTS: CONSTRUCTORS
  // *******************************************************************************************************************

  @Test
  public void testBaseConstructor() throws Exception {
    // Assert ScheduledExecutor is running
    assertNotNull(store.scheduledExecutor);
    assertFalse(store.scheduledExecutor.isShutdown());
    assertFalse(store.scheduledExecutor.isTerminated());

    // Assert StripedExecutor is running
    assertNotNull(store.stripedExecutor);
    assertFalse(store.stripedExecutor.isShutdown());
    assertFalse(store.stripedExecutor.isTerminated());

    assertNotNull(store.getArtifactIndex());

    assertEquals(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE, store.getThresholdWarcSize());

    assertEquals(
        WarcArtifactDataStore.DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION,
        store.getUncommittedArtifactExpiration()
    );
  }

  // *******************************************************************************************************************
  // * TESTS: DATA STORE LIFECYCLE
  // *******************************************************************************************************************

  @Test
  public void testInitDataStore() throws Exception {
    // Ignore the data store provided to us
    teardownDataStore();

    // Create a new index for the new data store below
    ArtifactIndex index = new VolatileArtifactIndex();
    index.initIndex();

    // Create a new data store for this test
    store = makeWarcArtifactDataStore(index);

    // Sanity check: Assert data store is using our provided index
    assertSame(index, store.getArtifactIndex());

    // Initialize the data store
    store.initDataStore();

    // Run implementation-specific post-initArtifactDataStore() tests
    testInitDataStoreImpl();

    // Shutdown the data store and index created earlier
    store.shutdownDataStore();
    index.shutdownIndex();
  }

  @Test
  public void testShutdownDataStore() throws Exception {
    testShutdownDataStoreImpl();
  }

  public void testShutdownDataStoreImpl() throws Exception {
    store.shutdownDataStore();

    assertTrue(store.scheduledExecutor.isShutdown());
    assertTrue(store.scheduledExecutor.isTerminated());

    assertTrue(store.stripedExecutor.isShutdown());
    assertTrue(store.stripedExecutor.isTerminated());

    assertEquals(WarcArtifactDataStore.DataStoreState.SHUTDOWN, store.getDataStoreState());
  }

  // *******************************************************************************************************************
  // * TESTS: INTERNAL PATH METHODS
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#getBasePathFromStorageUrl(URI)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetBasePathFromStorageUrl() throws Exception {
    final URI storageUrl = new URI("fake:///lockss/test/foo");

    // Mock
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    // Mock behavior
    doCallRealMethod().when(ds).getBasePathFromStorageUrl(storageUrl);

    // No match
    when(ds.getBasePaths()).thenReturn(new Path[]{Paths.get("/a"), Paths.get("/b")});
    assertNull(ds.getBasePathFromStorageUrl(storageUrl));

    // Match
    Path expectedPath = Paths.get("/lockss/test");
    when(ds.getBasePaths()).thenReturn(new Path[]{Paths.get("/lockss"), expectedPath});
    assertEquals(expectedPath, ds.getBasePathFromStorageUrl(storageUrl));
  }

  /**
   * Test for {@link WarcArtifactDataStore#isTmpStorage(Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testIsTmpStorage() throws Exception {
    // Mock
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    // Mock behavior
    Path[] tmpBasePaths = new Path[]{Paths.get("/a/tmp"), Paths.get("/b/tmp")};
    when(ds.getTmpWarcBasePaths()).thenReturn(tmpBasePaths);
    doCallRealMethod().when(ds).isTmpStorage(ArgumentMatchers.any(Path.class));

    // No match
    assertFalse(ds.isTmpStorage(Paths.get("/a/foo")));

    // Match
    assertTrue(ds.isTmpStorage(Paths.get("/a/tmp/foo/bar")));
    assertTrue(ds.isTmpStorage(Paths.get("/b/tmp/bar/foo")));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getBasePaths()}.
   *
   * @throws Exception
   */
  @Test
  public void testGetBasePaths() throws Exception {
    assertArrayEquals(expected_getBasePaths(), store.getBasePaths());
  }

  /**
   * Test for {@link WarcArtifactDataStore#getTmpWarcBasePaths()}.
   *
   * @throws Exception
   */
  @Test
  public void testGetTmpWarcBasePaths() throws Exception {
    assertNotNull(store.getTmpWarcBasePaths());
    assertArrayEquals(expected_getTmpWarcBasePaths(), store.getTmpWarcBasePaths());
  }

  /**
   * Test for {@link WarcArtifactDataStore#getCollectionsBasePath(Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetCollectionsBasePath() throws Exception {
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    doCallRealMethod().when(ds).getCollectionsBasePath(ArgumentMatchers.any(Path.class));

    Path basePath = Paths.get("/lockss");
    assertEquals(basePath.resolve(WarcArtifactDataStore.COLLECTIONS_DIR), ds.getCollectionsBasePath(basePath));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getCollectionPath(Path, String)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetCollectionPath() throws Exception {
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    doCallRealMethod().when(ds).getCollectionsBasePath(ArgumentMatchers.any(Path.class));
    doCallRealMethod().when(ds).getCollectionPath(ArgumentMatchers.any(Path.class), ArgumentMatchers.anyString());

    Path basePath = Paths.get("/lockss");
    String collectionId = "collection";

    Path expectedPath = basePath.resolve(WarcArtifactDataStore.COLLECTIONS_DIR).resolve(collectionId);
    assertEquals(expectedPath, ds.getCollectionPath(basePath, collectionId));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getAuPath(Path, String, String)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetAuPath() throws Exception {
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    doCallRealMethod().when(ds).getCollectionsBasePath(ArgumentMatchers.any(Path.class));

    doCallRealMethod().when(ds).getCollectionPath(ArgumentMatchers.any(Path.class), ArgumentMatchers.anyString());

    doCallRealMethod().when(ds).getAuPath(
        ArgumentMatchers.any(Path.class),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString()
    );

    Path basePath = Paths.get("/lockss");
    String collectionId = "collection";
    String auid = "auid";

    Path expectedAuPath = basePath
        .resolve(WarcArtifactDataStore.COLLECTIONS_DIR)
        .resolve(collectionId)
        .resolve(WarcArtifactDataStore.AU_DIR_PREFIX + DigestUtils.md5Hex(auid));

    assertEquals(expectedAuPath, ds.getAuPath(basePath, collectionId, auid));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getAuActiveWarcPath(String, String)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetAuActiveWarcPath() throws Exception {
    String collectionId = "collection";
    String auid = "auid";
    Path[] basePaths = new Path[]{Paths.get("/lockss")};

    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ds.auActiveWarcsMap = mock(Map.class);

    when(ds.getBasePaths()).thenReturn(basePaths);
    doCallRealMethod().when(ds).getAuActiveWarcPath(collectionId, auid);

    // Assert getAuActiveWarcPath() calls initAuActiveWarc() if there are no active WARCs for this AU
    when(ds.getAuActiveWarcPaths(collectionId, auid)).thenReturn(new Path[]{});
    ds.getAuActiveWarcPath(collectionId, auid);
    verify(ds).initAuActiveWarc(collectionId, auid);

    Path activeWarcA = mock(Path.class);
    Path activeWarcB = mock(Path.class);

    when(activeWarcA.getParent()).thenReturn(mock(Path.class));
    when(activeWarcB.getParent()).thenReturn(mock(Path.class));

    Path[] activeWarcs = new Path[]{activeWarcA, activeWarcB};
    when(ds.getAuActiveWarcPaths(collectionId, auid)).thenReturn(activeWarcs);

    // Assert largest is returned
    when(ds.getFreeSpace(activeWarcA.getParent())).thenReturn(1L);
    when(ds.getFreeSpace(activeWarcB.getParent())).thenReturn(0L);
    assertEquals(activeWarcA, ds.getAuActiveWarcPath(collectionId, auid));

    // Assert largest is returned
    when(ds.getFreeSpace(activeWarcA.getParent())).thenReturn(0L);
    when(ds.getFreeSpace(activeWarcB.getParent())).thenReturn(1L);
    assertEquals(activeWarcB, ds.getAuActiveWarcPath(collectionId, auid));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getAuActiveWarcPaths(String, String)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetAuActiveWarcPaths() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    ds.auActiveWarcsMap = new HashMap<>();
    String collectionId = "collection";
    String auid = "auid";

    // Mock behavior
    doCallRealMethod().when(ds).getAuActiveWarcPaths(collectionId, auid);

    // Assert empty array is key does not exist
    assertArrayEquals(new Path[]{}, ds.getAuActiveWarcPaths(collectionId, auid));

    // Setup activeWarcs list
    List<Path> activeWarcs = new ArrayList();
    activeWarcs.add(mock(Path.class));
    activeWarcs.add(mock(Path.class));

    // Inject value
    CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);
    ds.auActiveWarcsMap.put(key, activeWarcs);

    // Assert we get back the expected array
    assertArrayEquals(activeWarcs.toArray(new Path[0]), ds.getAuActiveWarcPaths(collectionId, auid));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getAuMetadataWarcPath(Path, ArtifactIdentifier, String)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetAuMetadataWarcPath() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    Path basePath = mock(Path.class);
    ArtifactIdentifier aid = mock(ArtifactIdentifier.class);

//    Path auPath = mock(Path.class);
    Path auPath = Paths.get("/lockss");

    // Mock behavior
    when(ds.getAuPath(basePath, aid.getCollection(), aid.getAuid())).thenReturn(auPath);
    doCallRealMethod().when(ds).getAuMetadataWarcPath(
        ArgumentMatchers.any(Path.class),
        ArgumentMatchers.any(ArtifactIdentifier.class),
        ArgumentMatchers.any(String.class)
    );

    // Call getAuMetadataWarcPath
    String journalName = "journal";
    Path journalPath = ds.getAuMetadataWarcPath(basePath, aid, journalName);

    // Assert expected path is resolved from auPath
    String journalFile = String.format("%s.%s", journalName, WarcArtifactDataStore.WARC_FILE_EXTENSION);
//    verify(auPath).resolve(journalFile);
//    verifyNoMoreInteractions(auPath);
    assertEquals(auPath.resolve(journalFile), journalPath);
  }

  // *******************************************************************************************************************
  // * INTERNAL STORAGE URL
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#makeWarcRecordStorageUrl(Path, long, long)}.
   *
   * @throws Exception
   */
  @Test
  public void testMakeWarcRecordStorageUrl() throws Exception {
    // Mock
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    // Mock behavior
    doCallRealMethod().when(ds).makeWarcRecordStorageUrl(
        ArgumentMatchers.any(Path.class),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong()
    );

    // Mock arguments
    Path filePath = mock(Path.class);
    long offset = 1234L;
    long length = 5678L;

    // Call makeWarcStorageUrl(Path, long long)
    ds.makeWarcRecordStorageUrl(filePath, offset, length);

    // Expected MultiValueMap param to makeStorageUrl
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add("offset", Long.toString(offset));
    params.add("length", Long.toString(length));

    // Assert that it delegates to implementation-specific makeStorageUrl(Path, MultiValueMap)
    verify(ds).makeStorageUrl(filePath, params);
  }

  /**
   * Test for {@link WarcArtifactDataStore#makeWarcRecordStorageUrl(WarcArtifactDataStore.WarcRecordLocation)}.
   *
   * @throws Exception
   */
  @Test
  public void testMakeWarcRecordStorageUrl_WarcRecordLocation() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    WarcArtifactDataStore.WarcRecordLocation recordLocation = mock(WarcArtifactDataStore.WarcRecordLocation.class);

    // Mock behavior
    doCallRealMethod().when(ds).makeWarcRecordStorageUrl(recordLocation);

    // Call makeWarcRecordStorageUrl(WarcRecordLocation)
    ds.makeWarcRecordStorageUrl(recordLocation);

    // Assert the method under test invokes makeWarcRecordStorageUrl(Path, long, long)
    verify(ds).makeWarcRecordStorageUrl(
        recordLocation.getPath(),
        recordLocation.getOffset(),
        recordLocation.getLength()
    );
  }

  /**
   * Test for {@link WarcArtifactDataStore#getPathFromStorageUrl(URI)}.
   *
   * @throws Exception
   */
  @Deprecated
  @Test
  public void testGetPathFromStorageUrl() throws Exception {
    URI uri = new URI("fake:///lockss/test.warc");
    assertEquals(Paths.get(uri.getPath()), WarcArtifactDataStore.getPathFromStorageUrl(uri));
  }

  // *******************************************************************************************************************
  // * METHODS
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#markAndGetInputStream(Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testMarkAndGetInputStream() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    Path filePath = mock(Path.class);

    // Mock behavior
    doCallRealMethod().when(ds).markAndGetInputStream(filePath);

    // Assert markAndGetInputStreamAndSeek(filePath, 0L) is called
    ds.markAndGetInputStream(filePath);
    verify(ds).markAndGetInputStreamAndSeek(filePath, 0L);
  }

  /**
   * Test for {@link WarcArtifactDataStore#markAndGetInputStreamAndSeek(Path, long)}.
   *
   * @throws Exception
   */
  @Test
  public void testMarkAndGetInputStreamAndSeek() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    Path filePath = mock(Path.class);
    long offset = 1234L;

    // Mock behavior
    doCallRealMethod().when(ds).markAndGetInputStreamAndSeek(
        ArgumentMatchers.any(Path.class),
        ArgumentMatchers.anyLong()
    );

    // Open input stream and assert file is marked in-use
    InputStream input = ds.markAndGetInputStreamAndSeek(filePath, offset);
    assertTrue(TempWarcInUseTracker.INSTANCE.isInUse(filePath));

    // Close input stream and assert file is marked not in-use
    input.close();
    assertFalse(TempWarcInUseTracker.INSTANCE.isInUse(filePath));
  }

  // *******************************************************************************************************************
  // * AU ACTIVE WARCS LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#generateActiveWarcName(String, String, ZonedDateTime)}.
   *
   * @throws Exception
   */
  @Test
  public void testGenerateActiveWarcName() throws Exception {
    String collectionId = "collection";
    String auid = "auid";
    ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("UTC"));

    // Expected active WARC file name
    String timestamp = zdt.format(WarcArtifactDataStore.FMT_TIMESTAMP);
    String auidHash = DigestUtils.md5Hex(auid);
    String expectedName = String.format("artifacts_%s-%s_%s.warc", collectionId, auidHash, timestamp);

    // Assert generated active WARC file name matches expected file name
    assertEquals(expectedName, WarcArtifactDataStore.generateActiveWarcName(collectionId, auid, zdt));
  }

  /**
   * Test for {@link WarcArtifactDataStore#initAuActiveWarc(String, String)}.
   *
   * @throws Exception
   */
  @Test
  public void testInitAuActiveWarc() throws Exception {
    String collectionId = "collection";
    String auid = "auid";

    // Mock
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    // Mock behavior
    ds.auActiveWarcsMap = new HashMap<>();
    doCallRealMethod().when(ds).initAuActiveWarc(collectionId, auid);
//    doCallRealMethod().when(ds).getAuPaths(collectionId, auid);
//    doCallRealMethod().when(ds).getAuPath(ArgumentMatchers.any(Path.class), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
//    when(ds.getBasePaths()).thenReturn(new Path[]{});

    // Assert IllegalStateException thrown if no base paths configured (simulated by having getAuPaths() return null)
    when(ds.getAuPaths(collectionId, auid)).thenReturn(new Path[]{});
    assertThrows(IllegalStateException.class, () -> ds.initAuActiveWarc(collectionId, auid));

    // Setup getAuPaths() to return AU paths
    Path auPathA = Paths.get("/a/lockss/collections/collection/auid");
    Path auPathB = Paths.get("/b/lockss/collections/collection/auid");

    when(ds.getFreeSpace(auPathA)).thenReturn(0L);
    when(ds.getFreeSpace(auPathB)).thenReturn(1L);
    when(ds.getAuPaths(collectionId, auid)).thenReturn(new Path[]{auPathA, auPathB});

    // Call initAuActiveWarc()
    Path activeWarc = ds.initAuActiveWarc(collectionId, auid);

    log.trace("activeWarc = {}", activeWarc);

    // Assert active WARC was initialized in expected AU path (i.e., the one with most free space)
    assertTrue(activeWarc.startsWith(auPathB));

    // Assert active WARCs list for this AU contains the new active WARC
    CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);
    List<Path> activeWarcs = ds.auActiveWarcsMap.get(key);
    assertNotNull(activeWarcs);
    assertEquals(1, activeWarcs.size());
    assertTrue(activeWarcs.contains(activeWarc));
  }

  /**
   * Test for {@link WarcArtifactDataStore#sealActiveWarc(String, String, Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testSealActiveWarc() throws Exception {
    String collectionId = "collection";
    String auid = "auid";
    CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);

    // Mock
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    // Mock behavior
    ds.auActiveWarcsMap = new HashMap<>();
    doCallRealMethod().when(ds).sealActiveWarc(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(Path.class)
    );

    // "WARN: Attempted to seal an active WARC of an AU having no active WARCs!"
    ds.sealActiveWarc(collectionId, auid, mock(Path.class));

    // "WARN: Attempted to seal an active WARC of an AU that is not active!"
    ds.auActiveWarcsMap.put(key, new ArrayList<>());
    ds.sealActiveWarc(collectionId, auid, mock(Path.class));

    // Inject active WARCs list for this AU
    Path activeWarc = mock(Path.class);
    List<Path> activeWarcs = new ArrayList<>();
    activeWarcs.add(activeWarc);
    ds.auActiveWarcsMap.put(key, activeWarcs);

    // Assert sealing the active WARC succeeds
    ds.sealActiveWarc(collectionId, auid, activeWarc);
    assertFalse(ds.auActiveWarcsMap.get(key).contains(activeWarc));
  }

  // *******************************************************************************************************************
  // * TEMPORARY WARCS LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#runGarbageCollector()}.
   *
   * @throws Exception
   */
  @Test
  public void testRunGarbageCollector() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ds.scheduledExecutor = mock(ScheduledExecutorService.class);

    // Mock behavior
    doCallRealMethod().when(ds).runGarbageCollector();

    // Call runGarbageCollector()
    ds.runGarbageCollector();

    // Assert instance of GarbageCollectTempWarcsTask is submitted to scheduledExecutor
    verify(ds.scheduledExecutor).submit(ArgumentMatchers.any(WarcArtifactDataStore.GarbageCollectTempWarcsTask.class));
    verifyNoMoreInteractions(ds.scheduledExecutor);
  }

  /**
   * Test for {@link WarcArtifactDataStore#garbageCollectTempWarcs()}.
   *
   * @throws Exception
   */
  @Test
  public void testGarbageCollectTempWarcs() throws Exception {
    // Add an artifact from a new artifact specification
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();

    // Add an uncommitted artifact
    ArtifactData ad = spec.getArtifactData();
    Artifact addedArtifact = store.addArtifactData(ad);

    // Update artifact specification
    spec.setStorageUrl(URI.create(addedArtifact.getStorageUrl()));

    // Run garbage collector
    store.garbageCollectTempWarcs();

    // TODO: WIP. Finish.
  }

  /**
   * Asserts that an IllegalStateException is thrown when attempting to reload temporary WARCs with a null index.
   *
   * @throws Exception
   */
  @Test
  public void testReloadTempWarcs_nullIndex() throws Exception {
    // Cannot use the provided data store for this test
    teardownDataStore();

    // Assert that an IllegalStateException is thrown when attempting to reload temporary WARCs with a null index
    store = makeWarcArtifactDataStore(null);
    assertThrows(IllegalStateException.class, () -> store.reloadTemporaryWarcs());
  }

  /**
   * Test for {@link WarcArtifactDataStore#reloadTemporaryWarcs()}.
   *
   * @throws Exception
   */
  @Test
  public void testReloadTempWarcs() throws Exception {
    // Do not use provided data store for this test
    teardownDataStore();

    runTestReloadTempWarcs(true, true, false);
    runTestReloadTempWarcs(true, false, false);
    runTestReloadTempWarcs(false, true, false);
    runTestReloadTempWarcs(false, false, false);

    runTestReloadTempWarcs(true, true, true);
    runTestReloadTempWarcs(true, false, true);
    runTestReloadTempWarcs(false, true, true);
    runTestReloadTempWarcs(false, false, true);
  }

  /**
   * Runs tests against the temporary WARC reloading mechanism of {@code WarcArtifactDataStore} implementations. It does
   * this by asserting against the effect of adding one artifact then reloading the temporary WARC.
   *
   * @param commit A {@code boolean} indicating whether the artifact should be committed.
   * @param expire A {@code boolean} indicating whether the artifact should be expired.
   * @throws Exception
   */
  private void runTestReloadTempWarcs(boolean commit, boolean expire, boolean delete) throws Exception {
    // Create and initialize a blank index
    ArtifactIndex index = new VolatileArtifactIndex();
    index.initIndex();

    // Instantiate a new data store with our newly instantiated volatile artifact index
    store = makeWarcArtifactDataStore(index);
    assertNotNull(store);
    assertSame(index, store.getArtifactIndex());

    // Get temporary WARCs directory storage path
    Path tmpWarcBasePath = store.getTmpWarcBasePaths()[0];

    // Garbage collector must not be running while reloading temporary WARCs so we do NOT initialize it
    //store.initDataStore();
    assertEquals(WarcArtifactDataStore.DataStoreState.UNINITIALIZED, store.getDataStoreState());

    // Assert empty temporary WARCs directory
    assertEquals(0, store.findWarcs(tmpWarcBasePath).size());

    // Add an artifact to the store and index
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();

    ArtifactData ad = spec.getArtifactData();

    Artifact storedArtifact = store.addArtifactData(ad);
    assertNotNull(storedArtifact);

    log.debug2("Finished add stage");

    // Get the artifact ID
    String artifactId = storedArtifact.getId();

    // Assert that the artifact exists in the index
    assertTrue(index.artifactExists(artifactId));

    Artifact artifact = index.getArtifact(artifactId);
    assertNotNull(artifact);

    // Assert that the artifact is not committed
    assertFalse(artifact.getCommitted());

    // Assert that the storage URL points to a WARC within the temporary WARCs directory
    assertTrue(WarcArtifactDataStore.getPathFromStorageUrl(new URI(artifact.getStorageUrl())).startsWith(tmpWarcBasePath));

    if (commit) {
      // Commit to artifact data store and index
      index.commitArtifact(storedArtifact.getId());
      Future<Artifact> future = store.commitArtifactData(storedArtifact);
      assertNotNull(future);

      // Wait for data store commit (copy from temporary to permanent storage) to complete - 10 seconds should be plenty
      artifact = future.get(10, TimeUnit.SECONDS);
      assertNotNull(artifact);
      assertTrue(artifact.getCommitted());

      // Assert that the storage URL now points to a WARC that is in permanent storage
      Path artifactWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(new URI(artifact.getStorageUrl()));
      assertTrue(!store.isTmpStorage(artifactWarcPath));
      assertTrue(isFile(artifactWarcPath));
    } else {
      // Assert that the storage URL points to a WARC within the temporary WARCs directory
      assertTrue(WarcArtifactDataStore.getPathFromStorageUrl(new URI(artifact.getStorageUrl())).startsWith(tmpWarcBasePath));
    }

    // Retrieve the artifact from the index
    artifact = index.getArtifact(artifactId);
    assertNotNull(artifact);
    // TODO: Compare with original artifact handle earlier

    // Assert commit status of artifact
    assertEquals(commit, artifact.getCommitted());

    // Assert one temporary WARC file has been created
    log.trace("tmpWarcBasePath = {}", tmpWarcBasePath);
    assertEquals(1, store.findWarcs(tmpWarcBasePath).size());

    log.debug2("Finished commit stage");

    if (delete) {
      // Delete the artifact
      store.deleteArtifactData(artifact);
      index.deleteArtifact(artifactId);

      // Assert that the artifact is removed from the data store and index
      assertTrue(store.isArtifactDeleted(artifact.getIdentifier()));
      assertFalse(index.artifactExists(artifactId));
      assertNull(index.getArtifact(artifactId));
    }

    log.debug2("Finished delete stage");

    log.info("Reloading WARC data store");

    // Restart WARC data store
    WADS reloadedStore = makeWarcArtifactDataStore(index, store);
    assertNotNull(reloadedStore);
    assertSame(store.getArtifactIndex(), reloadedStore.getArtifactIndex());
    assertEquals(store.getBasePaths(), reloadedStore.getBasePaths());

    if (expire) {
      // Set the data store to expire artifacts immediately
      reloadedStore.setUncommittedArtifactExpiration(0);
      assertEquals(0, reloadedStore.getUncommittedArtifactExpiration());
    }

    // Reload temporary WARCs
    reloadedStore.reloadTemporaryWarcs();

    // Scan directories for temporary WARC files and assert its state
    Collection<Path> tmpWarcs = reloadedStore.findWarcs(tmpWarcBasePath);

    // Determine artifact state
    ArtifactState artifactState = reloadedStore.getArtifactState(expire, delete, artifact);

    log.debug("commit = {}, expire = {}, delete = {}, artifactState = {}", commit, expire, delete, artifactState);

    switch (artifactState) {
      case NOT_INDEXED:
      case UNCOMMITTED:
      case COMMITTED:
        // The temporary WARC containing this artifact should NOT have been removed
        assertTrue(WarcArtifactDataStore.getPathFromStorageUrl(new URI(artifact.getStorageUrl())).startsWith(tmpWarcBasePath));
        assertEquals(1, tmpWarcs.size());
        break;

      case EXPIRED:
        // The temporary WARC containing only this artifact should have been removed
        assertEquals(0, tmpWarcs.size());
        break;

      case COPIED:
        // The temporary WARC containing only this artifact should have been removed
        assertEquals(0, tmpWarcs.size());

        // Artifact's storage URL should point to a WARC in permanent storage
        Path artifactWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(new URI(artifact.getStorageUrl()));
        assertTrue(!store.isTmpStorage(artifactWarcPath));
        break;

      case DELETED:
        // The temporary WARC containing only this artifact should have been removed
        assertEquals(0, tmpWarcs.size());

        assertTrue(reloadedStore.isArtifactDeleted(artifact.getIdentifier()));
        assertFalse(index.artifactExists(artifactId));
        assertNull(index.getArtifact(artifactId));
        break;
    }
  }

  /**
   * Test for {@link WarcArtifactDataStore#isTempWarcRemovable(Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testIsTempWarcRemovable() throws Exception {
    // TODO
  }

  /**
   * Test for {@link WarcArtifactDataStore#isTempWarcRecordRemovable(ArchiveRecord)}.
   *
   * @throws Exception
   */
  @Test
  public void testIsWarcRecordRemovable() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ArchiveRecord record = mock(ArchiveRecord.class);
    ArchiveRecordHeader header = mock(ArchiveRecordHeader.class);
    ds.artifactIndex = mock(ArtifactIndex.class);

    // Mock behavior
    doCallRealMethod().when(ds).isTempWarcRecordRemovable(ArgumentMatchers.any(ArchiveRecord.class));
    when(record.getHeader()).thenReturn(header);

    // Assert non-response/resource WARC record is removable
    for (WARCConstants.WARCRecordType type : WARCConstants.WARCRecordType.values()) {
      switch (type) {
        case resource:
        case response:
          // Ignore here - these cases are tested below to maintain code clarity
          continue;
        default:
          log.trace("type = {}", type);
          when(header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE)).thenReturn(type.toString());
          assertTrue(ds.isTempWarcRecordRemovable(record));
      }
    }

    // Mock behavior
    when(ds.artifactIndex.getArtifact((String) ArgumentMatchers.any())).thenReturn(mock(Artifact.class));

    // WARC record types that need further consideration
    WARCConstants.WARCRecordType[] types = new WARCConstants.WARCRecordType[]{
        WARCConstants.WARCRecordType.resource,
        WARCConstants.WARCRecordType.response
    };

    for (WARCConstants.WARCRecordType type : types) {
      log.trace("type = {}", type);

      when(header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE)).thenReturn(type.toString());

      for (ArtifactState state : ArtifactState.values()) {
        when(ds.getArtifactState(
            ArgumentMatchers.anyBoolean(),
            ArgumentMatchers.anyBoolean(),
            ArgumentMatchers.any(Artifact.class))
        ).thenReturn(state);

        // Assert expected return based on artifact state
        switch (state) {
          case EXPIRED:
          case COPIED:
          case DELETED:
            assertTrue(ds.isTempWarcRecordRemovable(record));
            continue;
          case NOT_INDEXED:
          case COMMITTED:
          case UNCOMMITTED:
          default:
            assertFalse(ds.isTempWarcRecordRemovable(record));
        }
      }
    }
  }

  // *******************************************************************************************************************
  // * ARTIFACT LIFECYCLE
  // ******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#getArtifactState(boolean, boolean, Artifact)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetArtifactState() throws Exception {
    // Do not use provided data store
    teardownDataStore();

    runTestGetArtifactState(true);
    runTestGetArtifactState(false);
  }

  /**
   * Runs tests for the determination of the life cycle state of an Artifact.
   *
   * @param expired A boolean indicating whether the artifact has expired.
   * @throws Exception
   */
  private void runTestGetArtifactState(boolean expired) throws Exception {
    // Configure WARC artifact data store with a newly instantiated volatile artifact index
    ArtifactIndex index = new VolatileArtifactIndex();

    // Instantiate a new WARC artifact data store for this run
    store = makeWarcArtifactDataStore(index);
    assertNotNull(store);
    assertEquals(index, store.getArtifactIndex());

    // Get the state of an artifact that has been deleted
    ArtifactState artifactState = store.getArtifactState(expired, true, null);
    log.trace("artifactState = {}", artifactState);
    assertEquals(ArtifactState.DELETED, artifactState);

    // Get the state of an artifact that has not been indexed.
    artifactState = store.getArtifactState(expired, false, null);
    log.trace("artifactState = {}", artifactState);

    // Verify.
    if (expired) {
      assertEquals(ArtifactState.EXPIRED, artifactState);
    } else {
      assertEquals(ArtifactState.NOT_INDEXED, artifactState);
    }

    // Initialize data store and index
    index.initIndex();
    store.initDataStore();

    // Add an artifact to the store and index
    ArtifactData ad = generateTestArtifactData("coll", "auid", "uri", 1, 512);
    Artifact artifact = store.addArtifactData(ad);

    // Get the artifact state.
    artifactState = store.getArtifactState(expired, false, artifact);
    log.trace("artifactState = {}", artifactState);

    // Verify.
    if (expired) {
      assertEquals(ArtifactState.EXPIRED, artifactState);
    } else {
      assertEquals(ArtifactState.UNCOMMITTED, artifactState);
    }

    // Commit the artifact.
    artifact.setCommitted(true);

    // Verify.
    assertEquals(ArtifactState.COMMITTED,
        store.getArtifactState(expired, false, artifact));

    // Commit to artifact data store
    Future<Artifact> artifactFuture = store.commitArtifactData(artifact);
    assertNotNull(artifactFuture);

    // Wait for data store commit (copy from temporary to permanent storage) to complete
    artifact = artifactFuture.get(10, TimeUnit.SECONDS);
    assertNotNull(artifact);
    assertTrue(artifact.getCommitted());

    // Verify.
    assertEquals(ArtifactState.COPIED, store.getArtifactState(expired, false, artifact));

    // Shutdown data store and index
    store.shutdownDataStore();
    index.shutdownIndex();
  }

  /**
   * Test for {@link WarcArtifactDataStore#isArtifactExpired(ArchiveRecord)}.
   *
   * @throws Exception
   */
  @Test
  public void testIsArtifactExpired() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ArchiveRecord record = mock(ArchiveRecord.class);
    ArchiveRecordHeader header = mock(ArchiveRecordHeader.class);

    doCallRealMethod().when(ds).isArtifactExpired(record);

    // Set WARC-Date field to now()
    when(record.getHeader()).thenReturn(header);
    when(header.getDate()).thenReturn(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));

    // Assert artifact is not expired if it expires a second later
    when(ds.getUncommittedArtifactExpiration()).thenReturn(1000L);
    assertFalse(ds.isArtifactExpired(record));

    // Assert artifact is expired if it expired a second ago
    when(ds.getUncommittedArtifactExpiration()).thenReturn(-1000L);
    assertTrue(ds.isArtifactExpired(record));
  }

  /**
   * Test for {@link WarcArtifactDataStore#isArtifactDeleted(ArtifactIdentifier)}.
   *
   * @throws Exception
   */
  @Test
  public void testIsArtifactDeleted() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ArtifactIdentifier aid = mock(ArtifactIdentifier.class);
    RepositoryArtifactMetadata metadata = mock(RepositoryArtifactMetadata.class);

    doCallRealMethod().when(ds).isArtifactDeleted(aid);

    // Assert if there is no journal entry for this artifact it is assumed to be deleted
    when(ds.getRepositoryMetadata(aid)).thenReturn(null);
    assertTrue(ds.isArtifactDeleted(aid));

    // Setup journal entry return
    when(ds.getRepositoryMetadata(aid)).thenReturn(metadata);

    // Assert return from isArtifactDeleted() matches journal
    when(metadata.isDeleted()).thenReturn(false);
    assertFalse(ds.isArtifactDeleted(aid));
    when(metadata.isDeleted()).thenReturn(true);
    assertTrue(ds.isArtifactDeleted(aid));
  }

  /**
   * Test for {@link WarcArtifactDataStore#isArtifactCommitted(ArtifactIdentifier)}.
   *
   * @throws Exception
   */
  @Test
  public void testIsArtifactCommitted() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ArtifactIdentifier aid = mock(ArtifactIdentifier.class);
    RepositoryArtifactMetadata metadata = mock(RepositoryArtifactMetadata.class);

    doCallRealMethod().when(ds).isArtifactCommitted(aid);

    // Assert if there is no journal entry for this artifact, isArtifactCommitted is false
    when(ds.getRepositoryMetadata(aid)).thenReturn(null);
    assertFalse(ds.isArtifactCommitted(aid));

    // Setup journal entry return
    when(ds.getRepositoryMetadata(aid)).thenReturn(metadata);

    // Assert return from isArtifactCommitted() matches journal
    when(metadata.isCommitted()).thenReturn(false);
    assertFalse(ds.isArtifactCommitted(aid));
    when(metadata.isCommitted()).thenReturn(true);
    assertTrue(ds.isArtifactCommitted(aid));
  }

  // *******************************************************************************************************************
  // * GETTERS AND SETTERS
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#setThresholdWarcSize(long)}.
   *
   * @throws Exception
   */
  @Test
  public void testSetThresholdWarcSize() throws Exception {
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    doCallRealMethod().when(ds).setThresholdWarcSize(ArgumentMatchers.anyLong());

    // Assert IllegalArgumentException thrown for negative WARC size
    assertThrows(IllegalArgumentException.class, () -> ds.setThresholdWarcSize(-1));
  }

  // *******************************************************************************************************************
  // * ArtifactDataStore INTERFACE IMPLEMENTATION
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#addArtifactData(ArtifactData)}.
   *
   * @throws Exception
   */
  @Test
  public void testAddArtifactData_null() throws Exception {
    try {
      Artifact artifact = store.addArtifactData(null);
      fail("Expected an IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("Null artifact data", e.getMessage());
    }

    try {
      ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
      spec.generateContent();
      ArtifactData ad = spec.getArtifactData();
      ad.setIdentifier(null);
      Artifact artifact = store.addArtifactData(ad);
      fail("Expected an IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("Artifact data has null identifier", e.getMessage());
    }
  }

  /**
   * Test for {@link WarcArtifactDataStore#addArtifactData(ArtifactData)}.
   *
   * @throws Exception
   */
  @VariantTest
  @EnumSource(TestRepoScenarios.class)
  public void testAddArtifactData_success() throws Exception {
    // Assert variant state against data store
    assertVariantState();

    // Create a new artifact specification
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();

    // Add the artifact data
    Artifact addedArtifact = store.addArtifactData(spec.getArtifactData());
    assertNotNull(addedArtifact);

    // Update the spec from the addArtifactData() operation
    spec.setStorageUrl(URI.create(addedArtifact.getStorageUrl()));

    // Assert things about the artifact we got back
    log.trace("addedArtifact = {}", addedArtifact);
    spec.assertArtifact(store, addedArtifact);

    // Assert newly added artifact is uncommitted
    assertFalse(addedArtifact.getCommitted());

    assertEquals(spec.getContentLength(), addedArtifact.getContentLength());
    assertEquals(spec.getContentDigest(), addedArtifact.getContentDigest());

    // Assert temporary WARC directory exists
    assertTrue(isDirectory(store.getTmpWarcBasePaths()[0]));

    // Assert things about the artifact's storage URL
    String storageUrl = addedArtifact.getStorageUrl();
    assertNotNull(storageUrl);
    assertFalse(storageUrl.isEmpty());

    Path artifactWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(new URI(storageUrl));
    log.debug("storageUrl = {}", storageUrl);
    log.debug("artifactWarcPath = {}", artifactWarcPath);
    assertTrue(isFile(artifactWarcPath));

    assertNotNull(store.getTmpWarcBasePaths());

    assertTrue(store.isTmpStorage(artifactWarcPath));

    // Get handle to artifact index used by data store
    ArtifactIndex index = store.getArtifactIndex();
    assertNotNull(index);

    // Assert changes to the index
    assertTrue(index.artifactExists(addedArtifact.getId()));
    Artifact indexedArtifact = index.getArtifact(addedArtifact.getId());
    assertNotNull(indexedArtifact);
  }

  /**
   * Test for {@link WarcArtifactDataStore#getArtifactData(Artifact)}.
   *
   * @throws Exception
   */
  @VariantTest
  @EnumSource(TestRepoScenarios.class)
  public void testGetArtifactData_withVariants() throws Exception {
    // Get artifact from index
    ArtifactIndex index = store.getArtifactIndex();

    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      String artifactId = spec.getArtifactId();

      log.debug("artifactId: {}, spec.isDeleted(): {}", artifactId, spec.isDeleted());

      Artifact artifact = index.getArtifact(spec.getArtifactId());

      if (spec.isDeleted()) {
        // Yes: Assert against deleted artifact
        assertNull(artifact, String.format("Deleted artifact still in index [artifactId: %s]", artifactId));

        assertFalse(index.artifactExists(artifactId));

        // Assert against data store
        assertTrue(store.isArtifactDeleted(spec.getArtifactIdentifier()));
        ArtifactData ad = store.getArtifactData(spec.getArtifact());
        assertNull(ad);
      } else {
        // No: Assert status of artifact in data store and index
        assertNotNull(artifact, String.format("Could not find artifact in index [artifactId: %s]", artifactId));

        // Assert artifact (from index) against spec
        spec.assertArtifact(store, artifact);

        // Assert artifact data against spec
        ArtifactData ad = store.getArtifactData(artifact);
        spec.assertArtifactData(ad);
      }
    }
  }

  /**
   * Test for {@link WarcArtifactDataStore#getArtifactData(Artifact)}.
   *
   * @throws Exception
   */
  @VariantTest
  @EnumSource(TestRepoScenarios.class)
  public void testGetArtifactData_badInput() throws Exception {
    // Attempt retrieving an artifact with a null argument
    assertThrows(IllegalArgumentException.class, () -> store.getArtifactData(null));

    // Attempt retrieving artifacts that do not exist
    for (ArtifactSpec spec : neverFoundArtifactSpecs) {
      // Update spec
      if (spec.getArtifactId() == null) {
        spec.setArtifactId(UUID.randomUUID().toString());
      }

      spec.generateContent();
      spec.setStorageUrl(URI.create("bad"));

      log.debug("Generated content for bogus artifact [artifactId: {}]", spec.getArtifactId());

      // Assert that getArtifactData() returns null if it
      assertNull(store.getArtifactData(spec.getArtifact()));
    }

    // Get a handle to the data store's artifact index
    ArtifactIndex index = store.getArtifactIndex();
    assertNotNull(index);

    // Attempt retrieving an artifact that exists but has a malformed storage URL
    ArtifactSpec spec = variantState.anyUncommittedSpec();
    if (spec != null) {
      if (!spec.isDeleted()) {
        log.debug("spec = {}", spec);
        log.debug("artifactExists = {}", index.artifactExists(spec.getArtifactId()));
        index.updateStorageUrl(spec.getArtifactId(), "bad url");
        spec.setStorageUrl(URI.create("bad"));
        assertThrows(IllegalArgumentException.class, () -> store.getArtifactData(spec.getArtifact()));
      }
    }

    // TODO: Attempt retrieving an artifact that exists and has a valid storage URL, but causes getArtifactData to
    //       handle an IOException.

    // TODO: Test other cases of wrong-ness?
  }

  /**
   * Test for {@link WarcArtifactDataStore#updateArtifactMetadata(Path, ArtifactIdentifier, RepositoryArtifactMetadata)}.
   *
   * @throws Exception
   */
  @VariantTest
  @EnumSource(TestRepoScenarios.class)
  public void testUpdateArtifactMetadata() throws Exception {
    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      RepositoryArtifactMetadata metadata = store.getRepositoryMetadata(spec.getArtifactIdentifier());
      assertEquals(spec.isCommitted(), metadata.isCommitted());
      assertEquals(spec.isDeleted(), metadata.isDeleted());
    }

    ArtifactIdentifier identifier = new ArtifactIdentifier("aid", "cx", "ax", "ux", 1);

//    assertFalse(isFile(store.getAuMetadataWarcPath(identifier, RepositoryArtifactMetadata.getMetadataId())));

    runTestUpdateArtifactMetadata(false, false);
    runTestUpdateArtifactMetadata(false, true);
    runTestUpdateArtifactMetadata(true, false);
    runTestUpdateArtifactMetadata(true, true);
  }

  private void runTestUpdateArtifactMetadata(boolean committed, boolean deleted) throws Exception {
    // Create an ArtifactIdentifier to test with
    ArtifactIdentifier identifier = new ArtifactIdentifier("aid", "c", "a", "u", 1);
    RepositoryArtifactMetadata metadata = new RepositoryArtifactMetadata(identifier, committed, deleted);

    Path basePath = store.getBasePaths()[0];

    // Write metadata
    store.updateArtifactMetadata(basePath, identifier, metadata);

    // Assert metadata file exists
    assertTrue(isFile(store.getAuMetadataWarcPath(basePath, identifier, RepositoryArtifactMetadata.getMetadataId())));

    // Read and assert metadata
    RepositoryArtifactMetadata storedMetadata = store.getRepositoryMetadata(identifier);
    assertEquals(metadata.getArtifactId(), storedMetadata.getArtifactId());
    assertEquals(metadata.getCommitted(), storedMetadata.getCommitted());
    assertEquals(metadata.getDeleted(), storedMetadata.getDeleted());
  }

  /**
   * Test for {@link WarcArtifactDataStore#commitArtifactData(Artifact)}.
   *
   * @throws Exception
   */
  @VariantTest
  @EnumSource(TestRepoScenarios.class)
  public void testCommitArtifact() throws Exception {
    // Illegal arguments
    assertThrows(IllegalArgumentException.class, () -> store.commitArtifactData(null));

    // Assert the state of variant against the data store
    assertVariantState();

    // Assert attempting to commit a delete artifact results in null
    ArtifactSpec deletedSpec = variantState.anyDeletedSpec();
    if (deletedSpec != null) {
      assertNull(store.commitArtifactData(deletedSpec.getArtifact()));
    }

    // Double commit test
    ArtifactSpec committedSpec = variantState.anyCommittedSpec();
    if (committedSpec != null) {

      // Commit this committed artifact again
      Future<Artifact> future = store.commitArtifactData(committedSpec.getArtifact());

      if (future != null) {
        // Wait for async commit operation to complete
        Artifact committedArtifact = future.get(10, TimeUnit.SECONDS); // FIXME

        // Assert spec and committed artifact (nothing should have changed after double commit)
        committedSpec.assertArtifact(store, committedArtifact);
      } else {
        // Artifact could not be committed (because it is deleted)
        assertTrue(committedSpec.isDeleted());
      }
    }

    // Assert the variant state again
    assertVariantState();
  }

  /**
   * Test for {@link WarcArtifactDataStore#deleteArtifactData(Artifact)}.
   *
   * @throws Exception
   */
  @VariantTest
  @EnumSource(TestRepoScenarios.class)
  public void testDeleteArtifactData() throws Exception {
    // Attempt to delete with a null artifact; assert we get back an IllegalArgumentException
    assertThrows(IllegalArgumentException.class, () -> store.deleteArtifactData(null));

    // Attempt to delete an artifact that does not exist
    for (ArtifactSpec spec : neverFoundArtifactSpecs) { // FIXME
      spec.setArtifactId(UUID.randomUUID().toString());
      spec.generateContent();
      spec.setStorageUrl(URI.create("bad"));

      store.deleteArtifactData(spec.getArtifact());
    }

    // Assert variant state
    assertVariantState();

    // Create an artifact specification
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
    spec.setArtifactId(UUID.randomUUID().toString());
    spec.generateContent();

    // Add the artifact data from the specification to the data store
    Artifact artifact = store.addArtifactData(spec.getArtifactData());
    assertNotNull(artifact);

    // Delete the artifact from the artifact store
    store.deleteArtifactData(artifact);

    // Assert attempt to retrieve the deleted artifact data results in a null
    assertNull(store.getArtifactData(artifact));

    // Verify that the repository metadata journal and index reflect the artifact is deleted
    assertTrue(store.isArtifactDeleted(spec.getArtifactIdentifier()));
    assertNull(store.getArtifactIndex().getArtifact(artifact.getId()));
  }

  /**
   * Test for {@link WarcArtifactDataStore#getInputStreamFromStorageUrl(URI)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetInputStreamFromStorageUrl() throws Exception {
    // TODO
  }

  // *******************************************************************************************************************
  // * INDEX REBUILD FROM DATA STORE
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#rebuildIndex(ArtifactIndex)}.
   *
   * @throws Exception
   */
  @Test
  public void testRebuildIndex() throws Exception {
    // Don't use provided data store, which provides an volatile index set
    teardownDataStore();

    // Instances of artifact index to populate and compare
    ArtifactIndex index1 = new VolatileArtifactIndex();
    ArtifactIndex index2 = new VolatileArtifactIndex();

    //// Create and populate first index by adding and indexing new artifacts
    store = makeWarcArtifactDataStore(index1);
    assertEquals(index1, store.getArtifactIndex());

    // Add first artifact to the repository - don't commit
    ArtifactData ad1 = generateTestArtifactData("collection1", "auid1", "uri1", 1, 1024);
    Artifact a1 = store.addArtifactData(ad1);
    assertNotNull(a1);

    // Add second artifact to the repository - commit
    ArtifactData ad2 = generateTestArtifactData("collection1", "auid1", "uri2", 1, 1024);
    Artifact a2 = store.addArtifactData(ad2);
    assertNotNull(a2);
    index1.commitArtifact(a2.getId());
    Future<Artifact> future = store.commitArtifactData(a2);
    assertNotNull(future);
    Artifact committed_a2 = future.get(10, TimeUnit.SECONDS);
    assertTrue(committed_a2.getCommitted());

    // Add third artifact to the repository - don't commit but immediately delete
    ArtifactData ad3 = generateTestArtifactData("collection1", "auid1", "uri3", 1, 1024);
    Artifact a3 = store.addArtifactData(ad3);
    assertNotNull(a3);
    store.deleteArtifactData(a3);
    index1.deleteArtifact(a3.getId());

    // Add fourth artifact to the repository - commit and delete
    ArtifactData ad4 = generateTestArtifactData("collection1", "auid1", "uri4", 1, 1024);
    Artifact a4 = store.addArtifactData(ad4);
    assertNotNull(a4);

    // Commit fourth artifact
    index1.commitArtifact(a4.getId());
    future = store.commitArtifactData(a4);
    assertNotNull(future);
    Artifact committed_a4 = future.get(10, TimeUnit.SECONDS);
    assertTrue(committed_a4.getCommitted());

    // Delete fourth artifact
    store.deleteArtifactData(a4);
    index1.deleteArtifact(a4.getId());

    // Shutdown the data store
    store.shutdownDataStore();

    //// Populate second index by rebuilding
    store = makeWarcArtifactDataStore(index2, store);
    assertEquals(index2, store.getArtifactIndex());
    store.rebuildIndex(index2);

    //// Compare and assert contents of indexes

    // Compare collections IDs
    List<String> cids1 = IterableUtils.toList(index1.getCollectionIds());
    List<String> cids2 = IterableUtils.toList(index2.getCollectionIds());
    assertIterableEquals(cids1, cids2);

    // Iterate over the collection IDs
    for (String cid : cids1) {
      // Compare the set of AUIDs
      List<String> auids1 = IteratorUtils.toList(index1.getAuIds(cid).iterator());
      List<String> auids2 = IteratorUtils.toList(index2.getAuIds(cid).iterator());
      assertIterableEquals(auids1, auids2);

      // Iterate over AUIDs
      for (String auid : auids1) {
        List<Artifact> artifacts1 = IteratorUtils.toList(index1.getArtifacts(cid, auid, true).iterator());
        List<Artifact> artifacts2 = IteratorUtils.toList(index2.getArtifacts(cid, auid, true).iterator());
        assertIterableEquals(artifacts1, artifacts2);
      }
    }
  }

  /**
   * Test for {@link WarcArtifactDataStore#reindexArtifactsFromWarcs(ArtifactIndex, Collection)}.
   * <p>
   * Q: What do we want to demonstrate here? It is parsing the storage URL using the static method
   * {@link WarcArtifactDataStore.WarcRecordLocation#fromStorageUrl(URI)} then calling
   * {@link WarcArtifactDataStore#getInputStreamAndSeek(Path, long)}.
   *
   * @throws Exception
   */
  @Test
  public void testReindexArtifactsFromWarcs() throws Exception {
    // TODO
  }

  // *******************************************************************************************************************
  // * JOURNAL OPERATIONS
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#truncateMetadataJournal(Path)}.
   * <p>
   * Q: What do we want to demonstrate here? This method should replace the journal file with a new file containing only
   * the most recent journal entry per artifact ID. It relies on {@link WarcArtifactDataStore#readMetadataJournal(Path)}
   * to read the journal and compile a {@link Map<String, JSONObject>} from artifact ID to most recent journal entry
   * i.e., a {@link JSONObject} object.
   * <p>
   * Each entry is deserialized into a {@link RepositoryArtifactMetadata} object then immediately serialized to the
   * journal.
   *
   * @throws Exception
   */
  @Test
  public void testTruncateMetadataJournal() throws Exception {
    Map<String, JSONObject> journal = new HashMap<>();
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    Path journalPath = mock(Path.class);

    // Mock behavior
    doCallRealMethod().when(ds).truncateMetadataJournal(journalPath);
    when(ds.readMetadataJournal(journalPath)).thenReturn(journal);
    when(ds.getAppendableOutputStream(journalPath)).thenReturn(output);

    // Call method
    ds.truncateMetadataJournal(journalPath);

    // TODO
  }

  /**
   * Test for {@link WarcArtifactDataStore#getRepositoryMetadata(ArtifactIdentifier)}.
   *
   * @throws Exception
   */
  @Test
  public void testGetRepositoryMetadata() throws Exception {
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    ArtifactIdentifier aid = mock(ArtifactIdentifier.class);
    when(aid.getId()).thenReturn("test");

    Path j1Path = mock(Path.class);
    Path j2Path = mock(Path.class);
    Path[] journalPaths = new Path[]{j1Path, j2Path};

    doCallRealMethod().when(ds).getRepositoryMetadata(aid);

    // Assert null return if no journals found
    when(ds.getAuMetadataWarcPaths(aid, RepositoryArtifactMetadata.LOCKSS_METADATA_ID)).thenReturn(new Path[]{});
    assertNull(ds.getRepositoryMetadata(aid));

    // Assert null return if journals do not contain an entry for this artifact
    when(ds.getAuMetadataWarcPaths(aid, RepositoryArtifactMetadata.LOCKSS_METADATA_ID)).thenReturn(journalPaths);
    assertNull(ds.getRepositoryMetadata(aid));

    // Assert expected entry returned
    Map<String, JSONObject> journal1 = new HashMap<>();
    JSONObject entry1 = new JSONObject("{artifactId: \"test\", committed: \"false\", deleted: \"false\"}");
    journal1.put(aid.getId(), entry1);
    when(ds.readMetadataJournal(j1Path)).thenReturn(journal1);
    assertEquals(new RepositoryArtifactMetadata(entry1), ds.getRepositoryMetadata(aid));

    // TODO: Right now getRepositoryMetadata() returns the first journal entry it comes across in the first journal file
    //       it come across. It would be robust if it compared by some sort of version or timestamp. Test that here:
//    Map<String, JSONObject> journal2 = new HashMap<>();
//    JSONObject entry2 = new JSONObject("{artifactId: \"test\", committed: \"true\", deleted: \"false\"}");
//    journal2.put(aid.getId(), entry2);
//    journal1.clear();
//    when(ds.readMetadataJournal(j1Path)).thenReturn(journal2);
//    assertEquals(new RepositoryArtifactMetadata(entry1), ds.getRepositoryMetadata(aid));
  }

  /**
   * Test for {@link WarcArtifactDataStore#readMetadataJournal(Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testReadMetadataJournal() throws Exception {
    WarcArtifactDataStore ds = spy(store);
    Path mockedJournalPath = mock(Path.class);

    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      ArtifactIdentifier aid = new ArtifactIdentifier("artifact", "collection", "auid", "url", 1);
      RepositoryArtifactMetadata artifactMetadata = new RepositoryArtifactMetadata(aid);

      // Append WARC metadata record to the journal
      WARCRecordInfo metadataRecord = WarcArtifactDataStore.createWarcMetadataRecord(aid.getId(), artifactMetadata);
      WarcArtifactDataStore.writeWarcRecord(metadataRecord, output);
      output.flush();

      doReturn(output.toInputStream()).when(ds).markAndGetInputStream(mockedJournalPath);

      Map<String, JSONObject> journal = ds.readMetadataJournal(mockedJournalPath);
      log.trace("journal = {}", journal);

      // TODO
    }
  }

  /**
   * Test for {@link WarcArtifactDataStore#replayRepositoryMetadata(ArtifactIndex, Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testReplayRepositoryMetadata() throws Exception {
    Map<String, JSONObject> journal = new HashMap<>();

    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);
    ArtifactIndex index = mock(ArtifactIndex.class);
    Path journalPath = mock(Path.class);

    // Mock behavior
    when(ds.readMetadataJournal(journalPath)).thenReturn(journal);
    doCallRealMethod().when(ds).replayRepositoryMetadata(index, journalPath);

    // "WARN: Artifact referenced by journal is not deleted but doesn't exist in index! [artifactId: test]"
    journal.put("test", new JSONObject("{artifactId: \"test\", committed: \"false\", deleted: \"false\"}"));
    ds.replayRepositoryMetadata(index, journalPath);

    // Nothing to do
    when(index.artifactExists("test")).thenReturn(true);
    ds.replayRepositoryMetadata(index, journalPath);

    // Trigger a commit replay
    journal.put("test", new JSONObject("{artifactId: \"test\", committed: \"true\", deleted: \"false\"}"));
    ds.replayRepositoryMetadata(index, journalPath);
    verify(index).commitArtifact("test");

    // Trigger a delete replay (but not a commit)
    clearInvocations(index);
    journal.put("test", new JSONObject("{artifactId: \"test\", committed: \"true\", deleted: \"true\"}"));
    ds.replayRepositoryMetadata(index, journalPath);
    verify(index).deleteArtifact("test");
    verify(index, never()).commitArtifact("test");
  }

  // *******************************************************************************************************************
  // * WARC
  // *******************************************************************************************************************

  @Test
  public void testWriteArtifactData() throws Exception {
    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
    ArtifactData ad = generateTestArtifactData("collection", "auid", "uri", 1, 1024);
    assertNotNull(ad);

    ArtifactIdentifier ai = ad.getIdentifier();
    assertNotNull(ai);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Serialize artifact data to byte stream
    store.writeArtifactData(ad, baos);

    if (log.isDebug2Enabled()) {
      log.debug2("str = {}", baos.toString());
    }

    // Transform WARC record byte stream to WARCRecord object
    WARCRecord record = new WARCRecord(new ByteArrayInputStream(baos.toByteArray()), getClass().getSimpleName(), 0);

    // Assert things about the WARC record
    assertNotNull(record);

    ArchiveRecordHeader headers = record.getHeader();
    assertNotNull(headers);

    // Assert mandatory WARC headers
    if (log.isDebug2Enabled()) {
      log.debug2("headers = {}", headers);
      log.debug2("headers.getUrl() = {}", headers.getUrl());

      log.debug2("headers.HEADER_KEY_ID = {}", headers.getHeaderValue(WARCConstants.HEADER_KEY_ID));
      log.debug2("headers.getRecordIdentifier() = {}", headers.getRecordIdentifier());
      log.debug2("headers.HEADER_KEY_ID = {}", headers.getReaderIdentifier());
    }

//    assertEquals(WarcArtifactDataStore.createRecordHeader());

    assertEquals(WARCConstants.WARCRecordType.response,
        WARCConstants.WARCRecordType.valueOf((String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE)));

    // Assert LOCKSS headers
    assertEquals(ai.getId(), headers.getHeaderValue(ArtifactConstants.ARTIFACT_ID_KEY));
    assertEquals(ai.getCollection(), headers.getHeaderValue(ArtifactConstants.ARTIFACT_COLLECTION_KEY));
    assertEquals(ai.getAuid(), headers.getHeaderValue(ArtifactConstants.ARTIFACT_AUID_KEY));
    assertEquals(ai.getUri(), headers.getHeaderValue(ArtifactConstants.ARTIFACT_URI_KEY));
    assertEquals(ai.getVersion(), Integer.valueOf((String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_VERSION_KEY)));
    assertEquals(ad.getContentLength(), Long.valueOf((String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_LENGTH_KEY)).longValue());

    // TODO: Assert content
  }

  @Test
  public void testCreateWarcMetadataRecord() throws Exception {
    // TODO
  }

  @Test
  public void testWriteWarcInfoRecord() throws Exception {
    // TODO
  }

  @Test
  public void testWriteWarcRecord() throws Exception {
    // TODO
  }

  @Test
  public void testFormatWarcRecordId() throws Exception {
    // TODO
  }

  @Test
  public void testCreateRecordHeader() throws Exception {
    // TODO
  }

  // *******************************************************************************************************************
  // *
  // *******************************************************************************************************************

  /**
   * Test for {@link WarcArtifactDataStore#getInputStreamAndSeek(Path, long)} and
   * {@link WarcArtifactDataStore#getAppendableOutputStream(Path)}.
   *
   * @throws Exception
   */
  @Test
  public void testInputOutputStreams() throws Exception {
    // Do not use the provided data store for this test
    teardownDataStore();

    // Create a new instance of the data store
    store = makeWarcArtifactDataStore(null);

    // Path to temporary WARC file
    String warcName = String.format("%s.%s", UUID.randomUUID(), WARCConstants.WARC_FILE_EXTENSION);
    Path warcPath = store.getTmpWarcBasePaths()[0].resolve(warcName);

    // Initialize WARC
    store.initWarc(warcPath);

    // Random offset and content length to write
    long offset = (long) new Random().nextInt((int) FileUtils.ONE_MB) + store.getWarcLength(warcPath);
    long length = (long) new Random().nextInt((int) FileUtils.ONE_KB);

    // Get an OutputStream
    OutputStream output = store.getAppendableOutputStream(warcPath);

    // Write padding to offset
    byte[] padding = new byte[(int) offset];
    Arrays.fill(padding, (byte) 0x00);
    output.write(padding);

    // Write random bytes
    byte[] expected = new byte[(int) length];
    new Random().nextBytes(expected);
    output.write(expected);
    output.flush();
    output.close();

    // Compare InputStreams
    InputStream is_actual = store.getInputStreamAndSeek(warcPath, offset);
    InputStream is_expected = new ByteArrayInputStream(expected);
    assertSameBytes(is_expected, is_actual);
  }

  @Test
  public void testGetSetUncommittedArtifactExpiration() {
    // Assert value of default constant
    assertEquals(TimeUtil.WEEK, WarcArtifactDataStore.DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION);

    // Assert the provided store received the default
    assertEquals(
        WarcArtifactDataStore.DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION,
        store.getUncommittedArtifactExpiration()
    );

    // Assert that an IllegalArgumentException is thrown if a negative expiration is thrown
    assertThrows(IllegalArgumentException.class, () -> store.setThresholdWarcSize(-1234L));

    // Assert setting artifacts to expire immediately is valid
    store.setUncommittedArtifactExpiration(0L);
    assertEquals(0L, store.getUncommittedArtifactExpiration());

    // Assert setting the expiration to some other time works
    store.setUncommittedArtifactExpiration(TimeUtil.DAY);
    assertEquals(TimeUtil.DAY, store.getUncommittedArtifactExpiration());
  }

  @Test
  public void testGetSetThresholdWarcSize() throws Exception {
    // Test default
    assertEquals(1L * FileUtils.ONE_GB, WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);
    assertEquals(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE, store.getThresholdWarcSize());

    // Test bad input
    assertThrows(IllegalArgumentException.class, () -> store.setThresholdWarcSize(-1234L));

    // Test for success
    store.setThresholdWarcSize(0L);
    assertEquals(0L, store.getThresholdWarcSize());
    store.setThresholdWarcSize(10L * FileUtils.ONE_KB);
    assertEquals(10L * FileUtils.ONE_KB, store.getThresholdWarcSize());
  }

//  @Test
//  public void testGetSetArtifactIndex() throws Exception {
//    // Don't use provided data store, which comes with an volatile index set
//    teardownDataStore();
//
//    // Create a new data store with null index
//    store = makeWarcArtifactDataStore(null);
//    assertNull(store.getArtifactIndex());
//
//    // Attempting to set the data store's artifact index to null should fail
//    assertThrows(IllegalArgumentException.class, () -> store.setArtifactIndex(null));
//
//    ArtifactIndex index1 = new VolatileArtifactIndex();
//    ArtifactIndex index2 = new VolatileArtifactIndex();
//
//    // Set the artifact index and check we get it back
//    store.setArtifactIndex(index1);
//    assertSame(index1, store.getArtifactIndex());
//
//    // Setting data store's index to the same index should be okay
//    store.setArtifactIndex(index1);
//    assertSame(index1, store.getArtifactIndex());
//
//    // Attempt to set to another index should fail
//    assertThrows(IllegalStateException.class, () -> store.setArtifactIndex(index2));
//  }

//  @Test
//  public void testMoveToPermanentStorage_null() throws Exception {
//    // Assert moving a null artifact results in an IllegalArgumentException
//    assertThrows(IllegalArgumentException.class, () -> store.moveToPermanentStorage(null));
//
//    // Assert bad storage URL results in an IllegalArgumentException
//    Artifact badArtifact = new Artifact();
//    badArtifact.setStorageUrl("fake");
//    assertThrows(IllegalArgumentException.class, () -> store.moveToPermanentStorage(badArtifact));
//  }
//
//  @Test
//  public void testMoveToPermanentStorage_badStorageUrls() throws Exception {
//    // Assert moving an artifact with a valid storage URL missing WARC results in a FileNotFound exception
//    Artifact badArtifact = new Artifact();
//    badArtifact.setCollection("fakeCollection");
//    badArtifact.setAuid("fakeAuid");
//    badArtifact.setStorageUrl("fakeStorageUrl?offset=1234&length=1234");
//    assertThrows(FileNotFoundException.class, () -> store.moveToPermanentStorage(badArtifact));
//
//    // TODO: Assert moving an artifact with a valid storage URL but bad offset results in XXX
//    // TODO: Assert moving an artifact with a valid storage URL but bad length results in XXX
//  }

//  @Test
//  public void testMoveToPermanentStorage_generic() throws Exception {
//    // Shutdown data store (a running GC will interfere with this test)
//    store.shutdownDataStore();
//    //store = makeWarcArtifactDataStore(new VolatileArtifactIndex());
//
//    ArtifactIndex index = store.getArtifactIndex();
//    assertNotNull(index);
//
//    // Add an artifact to the data store and index
//    ArtifactSpec spec = ArtifactSpec.forCollAuUrl(COLL1, AUID1, URL1);
//    ArtifactData ad = generateTestArtifactData("coll1", "auid1", "uri1", 1, 426);
//    Artifact artifact = store.addArtifactData(ad);
//
//    // Add an artifact to the data store and index
////    ArtifactData ad2 = generateTestArtifactData("coll1", "auid1", "uri1", 1, 426);
////    Artifact artifact2 = store.addArtifactData(ad2);
////    index.indexArtifact(ad2);
//
//    // Assert the storage URL points to a WARC in temporary storage
//    String beforeUrl = artifact.getStorageUrl();
//
//    if (log.isDebugEnabled()) {
//      log.debug("beforeUrl = {}", beforeUrl);
//      log.debug("getPathFromStorageUrl(beforeUrl) = {}", Artifact.getPathFromStorageUrl(beforeUrl));
//      log.debug("getTmpWarcBasePaths() = {}", store.getTmpWarcBasePaths());
//    }
//
//    assertTrue(store.isTmpStorage(Artifact.getPathFromStorageUrl(beforeUrl)));
//
//    // Move it to permanent storage
//    store.moveToPermanentStorage(artifact);
//
//    // Assert the storage URL points to the current active WARC for this AU
//    String afterUrl = artifact.getStorageUrl();
//    assertEquals(
//        store.getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid()),
//        Artifact.getPathFromStorageUrl(afterUrl)
//    );
//
//    // Assert successful copy of record
//    InputStream before_stream = store.getInputStreamFromStorageUrl(beforeUrl);
//    InputStream after_stream = store.getInputStreamFromStorageUrl(afterUrl);
//    assertSameBytes(before_stream, after_stream);
//  }

  @Test
  public void testMoveToPermanentStorage_sealFirst() throws Exception {
    assertNotNull(store.getArtifactIndex());

    // Set WARC file size threshold to 4KB
    store.setThresholdWarcSize(FileUtils.ONE_KB * 4L);
    store.getBlockSize();

    // TODO: Finish
  }

  @Deprecated
  @Test
  public void testSealActiveWarc_deprecated() throws Exception {
    // Constants for this test so that we can be consistent
    final String testCollection = "testCollection";
    final String testAuid = "testAuid";
    final String testUri = "testUri";

    // Asserting sealing an AU with no active WARCs results in no files being rewritten
    store.sealActiveWarc(testCollection, testAuid, null);
    assertEmpty(findWarcs(store.getAuPaths(testCollection, testAuid)));

    // Assert the active WARCs for this AU do not exist yet
//    for (Path activeWarcPath : store.getAuActiveWarcPaths(testCollection, testAuid)) {
//      assertFalse(pathExists(activeWarcPath));
//    }

    Path activeWarcPath = store.getAuActiveWarcPath(testCollection, testAuid);
    assertFalse(pathExists(activeWarcPath));

    // Generate and add an artifact
    ArtifactData artifactData = generateTestArtifactData(testCollection, testAuid, testUri, 1, 1234);
    Artifact artifact = store.addArtifactData(artifactData);

    // Commit the artifact
    Future<Artifact> future = store.commitArtifactData(artifact);
    Artifact committed = future.get(TIMEOUT_SHOULDNT, TimeUnit.MILLISECONDS);
    assertNotNull(committed);
    assertTrue(committed.getCommitted());

    // Assert the active WARC now exists
//    for (Path activeWarcPath : store.getAuActiveWarcPaths(testCollection, testAuid)) {
//      assertTrue(pathExists(activeWarcPath));
//    }
    activeWarcPath = store.getAuActiveWarcPath(testCollection, testAuid);
    assertTrue(pathExists(activeWarcPath));

    // Seal the AU's active WARCs
    store.sealActiveWarc(artifact.getCollection(), artifact.getAuid(), activeWarcPath);

    Iterable<Path> warcsBefore = findWarcs(store.getAuPaths(artifact.getCollection(), artifact.getAuid()));

    // Get the next active WARC path for this AU and assert it does not exist in storage
    Path nextActiveWarcPath = store.getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid());
    assertFalse(pathExists(nextActiveWarcPath));

    // Attempt to seal the AU's active WARC again
    store.sealActiveWarc(artifact.getCollection(), artifact.getAuid(), activeWarcPath);

    // Assert next active WARC still does not exist (since nothing was written to it before second seal)
    assertFalse(pathExists(nextActiveWarcPath));

    // Get set of WARC files in the AU directory after second seal
    Iterable<Path> warcsAfter = findWarcs(store.getAuPaths(artifact.getCollection(), artifact.getAuid()));

    // Assert the contents of the AU directory did not change after performing another seal
    assertIterableEquals(getURIsFromPaths(warcsBefore), getURIsFromPaths(warcsAfter));

    // Insert sleep for volatile data store (on fast machines there isn't enough resolution in the timestamp used in the
    // active WARC file name which causes the next and latest active WARC paths to match incorrectly)
    Thread.sleep(10);

    // Assert the next active WARC is unaffected
    Path latestActiveWarcPath = store.getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid());
    assertEquals(nextActiveWarcPath, latestActiveWarcPath);

    // Assert the new active WARC for this artifact's AU does not exist
    assertFalse(pathExists(latestActiveWarcPath));

    // Assert latest active WARC path are not the same as the original
    assertNotEquals(activeWarcPath, latestActiveWarcPath);
  }

  protected static Iterable<URI> getURIsFromPaths(Iterable<Path> paths) {
    return StreamSupport.stream(paths.spliterator(), true)
        .map(Path::toUri)
        .collect(Collectors.toList());
  }
}
