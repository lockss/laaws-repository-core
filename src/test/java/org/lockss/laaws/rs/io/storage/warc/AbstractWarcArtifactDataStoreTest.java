/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University,
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.laaws.rs.io.storage.warc;

import java.io.*;
import java.time.*;
import java.time.format.*;
import java.time.temporal.*;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.core.BaseLockssRepository;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.model.*;
import org.lockss.util.test.LockssTestCase5;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractWarcArtifactDataStoreTest<WADS extends WarcArtifactDataStore> extends LockssTestCase5 {
  private final static Log log = LogFactory.getLog(AbstractWarcArtifactDataStoreTest.class);

  protected WADS store;

  protected abstract WADS makeWarcArtifactDataStore() throws IOException;

  @BeforeEach
  public void setupTestContext() throws IOException {
    store = makeWarcArtifactDataStore();
  }

  @Test
  public void testGetBasePath() throws Exception { // FIXME
//    assertEquals(tmpRepoBaseDir.getAbsolutePath(), store.getBasePath());
  }

  @Test
  public void testSetThresholdWarcSize() throws Exception {
    assertEquals(100L * FileUtils.ONE_MB, WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);
    assertEquals(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE, store.getThresholdWarcSize());
    assertThrows(IllegalArgumentException.class, () -> store.setThresholdWarcSize(-1234L));
    store.setThresholdWarcSize(0L);
    assertEquals(0L, store.getThresholdWarcSize());
    store.setThresholdWarcSize(10L * FileUtils.ONE_KB);
    assertEquals(10L * FileUtils.ONE_KB, store.getThresholdWarcSize());
  }
  
  @Test
  public void testGetCollectionPath() throws Exception {
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", null, null, 0);
    assertEquals("/collections/coll1", store.getCollectionPath(ident1));
    assertEquals(store.getCollectionPath(ident1.getCollection()),
                 store.getCollectionPath(ident1));
  }
  
  @Test
  public void testGetAuPath() throws Exception {
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, 0);
    assertEquals("/collections/coll1/au-" + DigestUtils.md5Hex("auid1"),
                 store.getAuPath(ident1));
    assertEquals(store.getAuPath(ident1.getCollection(), ident1.getAuid()),
                 store.getAuPath(ident1));
  }
  
  @Test
  public void testGetSealedWarcPath() throws Exception {
    assertEquals("/sealed", store.getSealedWarcPath());
  }
  
  @Test
  public void testGetSealedWarcName() throws Exception {
    String warcName = store.getSealedWarcName("coll1", "auid1");
    assertThat(warcName, startsWith("coll1_au-" + DigestUtils.md5Hex("auid1") + "_"));
    assertThat(warcName, endsWith(".warc"));
    String timestamp = warcName.split("_")[2].split("artifacts.warc")[0];
    // DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS") does not parse in Java 8: https://bugs.openjdk.java.net/browse/JDK-8031085
    ZonedDateTime actual = ZonedDateTime.parse(timestamp, new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss").appendValue(ChronoField.MILLI_OF_SECOND, 3).toFormatter().withZone(ZoneId.of("UTC")));
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    assertTrue(actual.isAfter(now.minusSeconds(10L)) && actual.isBefore(now));
  }
  
  @Test
  public void testGetAuArtifactsWarcPath() throws Exception {
    // FIXME assertEquals(getBasePath() + expectedPath, getBasePath() + store.methodCall(...)) should be assertEquals(expectedPath, store.methodCall(...))
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, 0);
    String expectedAuDirPath = store.getBasePath() + "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    String expectedAuArtifactsWarcPath = expectedAuDirPath + "/artifacts.warc";
    assertFalse(pathExists(expectedAuDirPath)); // Not created until an artifact data is added
    assertEquals(expectedAuArtifactsWarcPath, store.getBasePath() + store.getAuArtifactsWarcPath(ident1));
    // FIXME assert that getAuArtifactsWarcPath() returns the same for ident1 and ident1.getCollection()+ident1.getAuid()
    // FIXME assert that the path exists now
  }
  
  @Test
  public void testGetAuMetadataWarcPath() throws Exception {
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, 0);
    RepositoryArtifactMetadata md1 = new RepositoryArtifactMetadata(ident1);
    String expectedAuBaseDirPath = "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    String expectedMetadataWarcPath = expectedAuBaseDirPath + "/lockss-repo.warc";
    assertFalse(pathExists(expectedAuBaseDirPath)); // Not created until an artifact data is added
    assertEquals(expectedMetadataWarcPath, store.getAuMetadataWarcPath(ident1, md1));
  }

  protected abstract boolean pathExists(String path) throws IOException;
  protected abstract boolean isDirectory(String path) throws IOException;
  protected abstract boolean isFile(String path) throws IOException;

  @Test
  public void testMakeStorageUrl() throws Exception {
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);
    String artifactsWarcPath = store.getAuArtifactsWarcPath(ident1);
    String expected = testMakeStorageUrl_getExpected(ident1, 1234L);
    String actual = store.makeStorageUrl(artifactsWarcPath, 1234L);
    assertEquals(expected, actual);
  }
  
  protected abstract String testMakeStorageUrl_getExpected(ArtifactIdentifier ident,
                                                           long offset)
      throws Exception;

  @Test
  public void testMakeNewStorageUrl() throws Exception {
    Artifact art1 = new Artifact();
    art1.setCollection("coll1");
    art1.setAuid("auid1");
    art1.setStorageUrl(store.makeStorageUrl("/original/path", 1234L));
    String actual = store.makeNewStorageUrl("/new/path", art1);
  }

  protected abstract Artifact testMakeNewStorageUrl_makeArtifactNotNeedingUrl(ArtifactIdentifier ident)
      throws Exception;
  
  protected abstract Artifact testMakeNewStorageUrl_makeArtifactNeedingUrl(ArtifactIdentifier ident)
      throws Exception;
  
  protected abstract void testMakeNewStorageUrl_checkArtifactNeedingUrl(Artifact artifact,
                                                                        String newPath,
                                                                        String result)
      throws Exception;


  protected static void quietlyDeleteDir(File dir) {
    try {
      FileUtils.deleteDirectory(dir);
    }
    catch (IOException ioe) {
      // oh well.
    }
  }

  @Test
  public void testRebuildIndex() throws Exception {
    // Instances of artifact index to populate and compare
    ArtifactIndex index1 = new VolatileArtifactIndex();
    ArtifactIndex index2 = new VolatileArtifactIndex();

    //// Create and populate first index by adding new artifacts to a repository
//    store.setArtifactIndex(index1);
    LockssRepository repository = new BaseLockssRepository(index1, store);

    // Add first artifact to the repository - don't commit
    ArtifactData ad1 = makeTestArtifactData(new ArtifactIdentifier("collection1", "auid1", "uri1", 1));
    Artifact a1 = repository.addArtifact(ad1);

    // Add second artifact to the repository - commit
    ArtifactData ad2 = makeTestArtifactData(new ArtifactIdentifier("collection1", "auid1", "uri2", 1));
    Artifact a2 = repository.addArtifact(ad2);
    repository.commitArtifact(a2);

    // Add third artifact to the repository - don't commit but immediately delete
    ArtifactData ad3 = makeTestArtifactData(new ArtifactIdentifier("collection1", "auid1", "uri3", 1));
    Artifact a3 = repository.addArtifact(ad3);
    repository.deleteArtifact(a3);

    // Add fourth artifact to the repository - commit and delete
    ArtifactData ad4 = makeTestArtifactData(new ArtifactIdentifier("collection1", "auid1", "uri4", 1));
    Artifact a4 = repository.addArtifact(ad4);
    repository.commitArtifact(a4);
    repository.deleteArtifact(a4);

    // Populate second index by rebuilding
    store.rebuildIndex(index2);

    //// Compare indexes

    // Compare collections IDs
    List<String> cids1 = IteratorUtils.toList(index1.getCollectionIds().iterator());
    List<String> cids2 = IteratorUtils.toList(index2.getCollectionIds().iterator());
    if (!(cids1.containsAll(cids2) && cids2.containsAll(cids1))) {
      fail("Expected both the original and rebuilt artifact indexes to contain the same set of collection IDs");
    }

    // Iterate over the collection IDs
    for (String cid : cids1) {
      // Compare the set of AUIDs
      List<String> auids1 = IteratorUtils.toList(index1.getAuIds(cid).iterator());
      List<String> auids2 = IteratorUtils.toList(index2.getAuIds(cid).iterator());
      if (!(auids1.containsAll(auids2) && auids2.containsAll(auids1))) {
        fail("Expected both the original and rebuilt artifact indexes to contain the same set of AUIDs");
      }

      // Iterate over AUIDs
      for (String auid : auids1) {
        List<Artifact> artifacts1 = IteratorUtils.toList(index1.getAllArtifacts(cid, auid, true).iterator());
        List<Artifact> artifacts2 = IteratorUtils.toList(index2.getAllArtifacts(cid, auid, true).iterator());

        // Debugging
        artifacts1.forEach(artifact -> log.info(String.format("Artifact from artifact1: %s", artifact)));
        artifacts2.forEach(artifact -> log.info(String.format("Artifact from artifact2: %s", artifact)));

        if (!(artifacts1.containsAll(artifacts2) && artifacts2.containsAll(artifacts1))) {
          fail("Expected both the original and rebuilt artifact indexes to contain the same set of artifacts");
        }
      }
    }
  }

  @Test
  public void testRebuildIndexSealed() throws Exception {
    // Instances of artifact index to populate and compare
    ArtifactIndex index3 = new VolatileArtifactIndex();
    ArtifactIndex index4 = new VolatileArtifactIndex();

    //// Create and populate first index by adding new artifacts to a repository
    LockssRepository repository = new BaseLockssRepository(index3, store);

    // The WARC records for the two artifacts here end up being 586 bytes each.
    store.setThresholdWarcSize(1024L);

    // HTTP status (200 OK) for use volatile ArtifactData's we'll add to the repository
    StatusLine status200 = new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    // Create an artifact and add it to the data store
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);
    org.apache.commons.io.output.ByteArrayOutputStream baos1 = new org.apache.commons.io.output.ByteArrayOutputStream(150);
    for (int i = 0 ; i < 150 ; ++i) {
      baos1.write('a');
    }
    ArtifactData dat1 = new ArtifactData(ident1, null, baos1.toInputStream(), status200);
    Artifact art1 = store.addArtifactData(dat1);
    baos1.close(); // to satisfy static analyzers

    // Register the artifact in the index
    index3.indexArtifact(dat1);
    repository.commitArtifact(art1);

    // Add another artifact to the store - this will add another 586 bytes while should trigger a seal
    ArtifactIdentifier ident2 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u2", 1);
    org.apache.commons.io.output.ByteArrayOutputStream baos2 = new ByteArrayOutputStream(150);
    for (int i = 0 ; i < 150 ; ++i) {
      baos2.write('b');
    }
    ArtifactData dat2 = new ArtifactData(ident2, null, baos2.toInputStream(), status200);
    Artifact art2 = store.addArtifactData(dat2);
    baos2.close(); // to satisfy static analyzers

    // Register the second artifact in the index
    index3.indexArtifact(dat2);
    repository.commitArtifact(art2);

    // Populate second index by rebuilding
    store.rebuildIndex(index4);

    //// Compare indexes

    // Compare collections IDs
    List<String> cids3 = IteratorUtils.toList(index3.getCollectionIds().iterator());
    List<String> cids4 = IteratorUtils.toList(index4.getCollectionIds().iterator());
    if (!(cids3.containsAll(cids4) && cids4.containsAll(cids3))) {
      fail("Expected both the original and rebuilt artifact indexes to contain the same set of collection IDs");
    }

    // Iterate over the collection IDs
    for (String cid : cids3) {
      // Compare the set of AUIDs
      List<String> auids3 = IteratorUtils.toList(index3.getAuIds(cid).iterator());
      List<String> auids4 = IteratorUtils.toList(index4.getAuIds(cid).iterator());
      if (!(auids3.containsAll(auids4) && auids4.containsAll(auids3))) {
        fail("Expected both the original and rebuilt artifact indexes to contain the same set of AUIDs");
      }

      // Iterate over AUIDs
      for (String auid : auids3) {
        List<Artifact> artifacts3 = IteratorUtils.toList(index3.getAllArtifacts(cid, auid, true).iterator());
        List<Artifact> artifacts4 = IteratorUtils.toList(index4.getAllArtifacts(cid, auid, true).iterator());

        // Debugging
        artifacts3.forEach(artifact -> log.info(String.format("Artifact from artifact1: %s", artifact)));
        artifacts4.forEach(artifact -> log.info(String.format("Artifact from artifact2: %s", artifact)));

        if (!(artifacts3.containsAll(artifacts4) && artifacts4.containsAll(artifacts3))) {
          fail("Expected both the original and rebuilt artifact indexes to contain the same set of artifacts");
        }
      }
    }
  }

  protected static ArtifactData makeTestArtifactData(ArtifactIdentifier ident) {
    InputStream is = new ByteArrayInputStream("whatever".getBytes());
    StatusLine status = new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");
    return new ArtifactData(ident, null, is, status);
  }

  @Test
  public void testWarcSealing() throws Exception {
    // Use a volatile artifact index with this data store
    ArtifactIndex index = new VolatileArtifactIndex();
    store.setArtifactIndex(index);

    // The WARC records for the two artifacts here end up being 586 bytes each.
    store.setThresholdWarcSize(1024L);

    // Setup repository paths relative to a base dir
    String auBaseDirPath = "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    String auArtifactsWarcPath = auBaseDirPath + "/artifacts.warc";
    String auMetadataWarcPath = auBaseDirPath + "/lockss-repo.warc";
    String sealedWarcDirPath = "/sealed";

    // Check that the repository is in an clean initialized state
    assertFalse(pathExists(auBaseDirPath));
    assertFalse(pathExists(auArtifactsWarcPath));
    assertFalse(pathExists(auMetadataWarcPath));
    assertTrue(isDirectory(sealedWarcDirPath));

    // HTTP status (200 OK) for use volatile ArtifactData's we'll add to the repository
    StatusLine status200 = new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    // Create an artifact and add it to the data store
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);
    org.apache.commons.io.output.ByteArrayOutputStream baos1 = new org.apache.commons.io.output.ByteArrayOutputStream(150);
    for (int i = 0 ; i < 150 ; ++i) {
      baos1.write('a');
    }
    ArtifactData dat1 = new ArtifactData(ident1, null, baos1.toInputStream(), status200);
    Artifact art1 = store.addArtifactData(dat1);
    baos1.close(); // to satisfy static analyzers

    // Register the artifact in the index
    index.indexArtifact(dat1);
    index.commitArtifact(art1.getId());
    assertNotNull(index.getArtifact(art1.getId()));

    // Directories for the AU should now exist
    assertTrue(isDirectory(auBaseDirPath));
    assertTrue(isFile(auArtifactsWarcPath));
    assertTrue(isFile(auMetadataWarcPath));
    assertTrue(isDirectory(sealedWarcDirPath));

    // The storage URL of the artifact data should match the storage url returned by artifact representing the artifact
    // data, and it should be belong to the correct AU's WARC file.
    assertEquals(dat1.getStorageUrl(), art1.getStorageUrl());
//    assertThat(art1.getStorageUrl(), startsWith(auArtifactsWarcPath));
    assertThat(art1.getStorageUrl(), startsWith(store.makeStorageUrl(auArtifactsWarcPath)));

    // Add another artifact to the store - this will add another 586 bytes while should trigger a seal
    ArtifactIdentifier ident2 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u2", 1);
    org.apache.commons.io.output.ByteArrayOutputStream baos2 = new ByteArrayOutputStream(150);
    for (int i = 0 ; i < 150 ; ++i) {
      baos2.write('b');
    }
    ArtifactData dat2 = new ArtifactData(ident2, null, baos2.toInputStream(), status200);
    Artifact art2 = store.addArtifactData(dat2);
    baos2.close(); // to satisfy static analyzers

    // Register the second artifact in the index
    index.indexArtifact(dat2);
    index.commitArtifact(art2.getId());
    assertNotNull(index.getArtifact(art2.getId()));

    // If seal was triggered, AU directory should exist but its default artifacts.warc should have been moved (i.e., no
    // longer exists at the original location)
    assertTrue(isDirectory(auBaseDirPath));
    assertFalse(pathExists(auArtifactsWarcPath));
    assertTrue(isFile(auMetadataWarcPath)); // TODO: What to do with the repository metadata? For now check that it's left in place
    assertTrue(isDirectory(sealedWarcDirPath));

    // ...the second artifact and its artifact data should point to a record in a sealed WARC
    assertEquals(dat2.getStorageUrl(), art2.getStorageUrl());

    // ...the sealed WARC should be located in the directory for sealed WARCs
//    assertThat(art2.getStorageUrl(), startsWith(sealedWarcDirPath));
    assertThat(art2.getStorageUrl(), startsWith(store.makeStorageUrl(sealedWarcDirPath)));

    // ...and the storage URL for the first artifact should have been updated
    Artifact art1i = index.getArtifact(art1.getId());
    assertNotNull(art1i);
    assertNotEquals(art1.getStorageUrl(), art1i.getStorageUrl());
//    assertThat(art1i.getStorageUrl(), startsWith(sealedWarcDirPath));
    assertThat(art1i.getStorageUrl(), startsWith(store.makeStorageUrl(sealedWarcDirPath)));
  }
  
}
