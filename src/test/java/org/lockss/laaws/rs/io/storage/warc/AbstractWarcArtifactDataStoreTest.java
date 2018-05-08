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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.storage.local.LocalWarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.util.test.LockssTestCase5;

public abstract class AbstractWarcArtifactDataStoreTest extends LockssTestCase5 {

  protected abstract WarcArtifactDataStore makeWarcArtifactDataStore(String repoBasePath) throws IOException;
  
  @Test
  public void testGetBasePath() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    assertEquals(tmp1.getAbsolutePath(), store.getBasePath());
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testSetThresholdWarcSize() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    // Use privileged access to peek
    assertEquals(100L * FileUtils.ONE_MB, WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);
    assertEquals(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE, store.thresholdWarcSize);
    assertThrows(IllegalArgumentException.class, () -> store.setThresholdWarcSize(-1234L));
    assertThrows(IllegalArgumentException.class, () -> store.setThresholdWarcSize(0L));
    store.setThresholdWarcSize(10L * FileUtils.ONE_KB);
    assertEquals(10L * FileUtils.ONE_KB, store.thresholdWarcSize);
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testGetCollectionPath() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", null, null, 0);
    assertEquals("/collections/coll1", store.getCollectionPath(ident1));
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testGetAuPath() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, 0);
    assertEquals("/collections/coll1/au-" + DigestUtils.md5Hex("auid1"),
                 store.getAuPath(ident1));
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testGetSealedWarcPath() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    assertEquals("/sealed", store.getSealedWarcPath());
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testGetSealedWarcName() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    String warcName = store.getSealedWarcName("coll1", "auid1");
    assertThat(warcName, startsWith("coll1_au-" + DigestUtils.md5Hex("auid1") + "_"));
    assertThat(warcName, endsWith(".warc"));
    String timestamp = warcName.split("_")[2].split(".warc")[0];
    // DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS") does not parse in Java 8: https://bugs.openjdk.java.net/browse/JDK-8031085
    ZonedDateTime actual = ZonedDateTime.parse(timestamp, new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss").appendValue(ChronoField.MILLI_OF_SECOND, 3).toFormatter().withZone(ZoneId.of("UTC")));
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    assertTrue(actual.isAfter(now.minusSeconds(10L)) && actual.isBefore(now));
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testGetAuArtifactsWarcPath() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, 0);
    String expectedAuDirPath = "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    File expectedRealAuDir = new File(store.getBasePath() + expectedAuDirPath);
    String expectedAuArtifactsWarcName = "artifacts.warc";
    String expectedAuArtifactsWarcPath = expectedAuDirPath + "/" + expectedAuArtifactsWarcName;
    assertFalse(expectedRealAuDir.exists());
    String actualAuArtifactsWarcPath = store.getAuArtifactsWarcPath(ident1);
    assertEquals(expectedAuArtifactsWarcPath, actualAuArtifactsWarcPath);
    assertTrue(expectedRealAuDir.isDirectory());
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testGetAuMetadataWarcPath() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, 0);
    RepositoryArtifactMetadata md1 = new RepositoryArtifactMetadata(ident1);
    String expectedAuPath = "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    File expectedRealAuDir = new File(store.getBasePath() + expectedAuPath);
    String expectedFileName = "lockss-repo.warc";
    String expectedPath = expectedAuPath + "/" + expectedFileName;
    assertFalse(expectedRealAuDir.exists());
    String actualPath = store.getAuMetadataWarcPath(ident1, md1);
    assertEquals(expectedPath, actualPath);
    assertTrue(expectedRealAuDir.isDirectory());
    quietlyDeleteDir(tmp1);
  }
  
  @Test
  public void testMakeStorageUrl() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);
    String artifactsWarcPath = store.getAuArtifactsWarcPath(ident1);
    String expected = testMakeStorageUrl_getExpected(store, ident1, 1234L);
    String actual = store.makeStorageUrl(artifactsWarcPath, 1234L);
    assertEquals(expected, actual);
    quietlyDeleteDir(tmp1);
  }
  
  protected abstract String testMakeStorageUrl_getExpected(WarcArtifactDataStore store,
                                                           ArtifactIdentifier ident,
                                                           long offset)
      throws Exception;

  @Test
  public void testMakeNewStorageUrl() throws Exception {
    File tmp1 = makeTempDir();
    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());

    Artifact art1 = new Artifact();
    art1.setCollection("coll1");
    art1.setAuid("auid1");
    art1.setStorageUrl(store.makeStorageUrl("/original/path", 1234L));
    String actual = store.makeNewStorageUrl("/new/path", art1);
    
    quietlyDeleteDir(tmp1);
  }

  protected abstract Artifact testMakeNewStorageUrl_makeArtifactNotNeedingUrl(WarcArtifactDataStore store,
                                                                              ArtifactIdentifier ident)
      throws Exception;
  
  protected abstract Artifact testMakeNewStorageUrl_makeArtifactNeedingUrl(WarcArtifactDataStore store,
                                                                           ArtifactIdentifier ident)
      throws Exception;
  
  protected abstract void testMakeNewStorageUrl_checkArtifactNeedingUrl(WarcArtifactDataStore store,
                                                                        Artifact artifact,
                                                                        String newPath,
                                                                        String result)
      throws Exception;
  
  protected File makeTempDir() throws IOException {
    File tempFile = File.createTempFile(getClass().getSimpleName(), null);
    tempFile.deleteOnExit();
    File tempDir = new File(tempFile.getAbsolutePath() + ".d");
    tempDir.mkdirs();
    return tempDir;
  }
  
  protected static void quietlyDeleteDir(File dir) {
    try {
      FileUtils.deleteDirectory(dir);
    }
    catch (IOException ioe) {
      // oh well.
    }
  }

}