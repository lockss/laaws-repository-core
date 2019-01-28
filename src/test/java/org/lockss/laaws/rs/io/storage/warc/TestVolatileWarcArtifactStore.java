/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.log.L4JLogger;

import java.io.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test class for {org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore}.
 */
public class TestVolatileWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<VolatileWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  private ArtifactIdentifier aid1;
  private ArtifactIdentifier aid2;
  private RepositoryArtifactMetadata md1;
  private RepositoryArtifactMetadata md2;
  private ArtifactData artifactData1;
  private ArtifactData artifactData2;

  private UUID uuid;
  private StatusLine httpStatus;

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore() throws IOException {
    return new VolatileWarcArtifactDataStore();
  }

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore(VolatileWarcArtifactDataStore other) throws IOException {
    return other;
  }

  @Override
  protected void runTestGetTmpWarcBasePath() {
    assertNotNull(store.getTmpWarcBasePath());
    assertEquals("/tmp", store.getTmpWarcBasePath());
  }

  @BeforeEach
  public void setUp() throws Exception {
    uuid = UUID.randomUUID();

    httpStatus = new BasicStatusLine(
            new ProtocolVersion("HTTP", 1,1),
            200,
            "OK"
    );

    aid1 = new ArtifactIdentifier("id1", "coll1", "auid1", "uri1", 1);
    aid2 = new ArtifactIdentifier(uuid.toString(), "coll2", "auid2", "uri2", 2);

    md1 = new RepositoryArtifactMetadata(aid1, false, false);
    md2 = new RepositoryArtifactMetadata(aid2, true, false);

    artifactData1 = new ArtifactData(aid1, null, new ByteArrayInputStream("bytes1".getBytes()), httpStatus, "surl1", md1);
    artifactData2 = new ArtifactData(aid2, null, new ByteArrayInputStream("bytes2".getBytes()), httpStatus, "surl2", md2);

    store = new VolatileWarcArtifactDataStore();
    store.initArtifactDataStore();
  }



  /*
  @Override
  @Test
  public void testGetAuArtifactsWarcPath() throws Exception {
//    File tmp1 = makeLocalTempDir();
//    WarcArtifactDataStore store = makeWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, null);
    String expectedAuDirPath = "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    String expectedAuArtifactsWarcName = store.getActiveWarcName("coll1", "auid1");
    String expectedAuArtifactsWarcPath = expectedAuDirPath + "/" + expectedAuArtifactsWarcName;
    String actualAuArtifactsWarcPath = store.getActiveWarcPath(ident1);
    assertEquals(expectedAuArtifactsWarcPath, actualAuArtifactsWarcPath);
//    quietlyDeleteDir(tmp1);
  }
  */
  
  @Override
  @Test
  public void testGetAuMetadataWarcPath() throws Exception {
//    File tmp1 = makeLocalTempDir();
//    WarcArtifactDataStore store = new LocalWarcArtifactDataStore(tmp1.getAbsolutePath());
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, null);
    RepositoryArtifactMetadata md1 = new RepositoryArtifactMetadata(ident1);
    String expectedAuDirPath = "/collections/coll1/au-" + DigestUtils.md5Hex("auid1");
    String expectedFileName = "lockss-repo.warc";
    String expectedPath = expectedAuDirPath + "/" + expectedFileName;
    String actualPath = store.getAuMetadataWarcPath(ident1, md1);
    assertEquals(expectedPath, actualPath);
//    quietlyDeleteDir(tmp1);
  }

  @Override
  protected boolean pathExists(String path) throws IOException {
    return isFile(path);
  }

  @Override
  protected boolean isDirectory(String path) throws IOException {
    return true;
  }

  @Override
  protected boolean isFile(String path) throws IOException {
    log.info("path = {}", path);

    Pattern p = Pattern.compile("^/collections/(.*)/(.*)/(.*)$");
    Matcher m = p.matcher(path);

    if (m.matches()) {
      String collection = m.group(1);
      String auid = m.group(2);
      String file = m.group(3);

      Map<String, Map<String, byte[]>> aus = store.repository.get(collection);
      Map<String, byte[]> au = aus.get(auid);

      log.info("au.get(file) = {}", au.get(file));

      return au.get(file) != null;
    } else if (path.startsWith(store.getTmpWarcBasePath())) {
      p = Pattern.compile("^" + store.getTmpWarcBasePath() + "/(.*)$");
      m = p.matcher(path);

      if (m.matches()) {
        return store.tempFiles.get(m.group(1)) != null;
      }
    }

    return false;
  }

  @Override
  protected String getAbsolutePath(String path) {
    return path;
  }

  @Override
  @Test
  @Disabled
  public void testMakeStorageUrl() throws Exception {
    
  }
  
  @Override
  protected String testMakeStorageUrl_getExpected(ArtifactIdentifier ident,
                                                  long offset)
      throws Exception {
    throw new UnsupportedOperationException();
  }
  
  @Disabled
  @Override
  @Test
  public void testReloadTempWarcs() throws Exception {
  }

  @Override
  protected boolean isValidStorageUrl(String storageUrl) {
    return true;
  }

  @Override
  @Test
  @Disabled
  public void testWarcSealing() throws Exception {
    // Intentionally left blank
  }

  @Override
  @Test
  @Disabled
  public void testRebuildIndex() throws Exception {
    // Intentionally left blank
  }

  @Override
  @Test
  @Disabled
  public void testRebuildIndexSealed() throws Exception {
    // Intentionally left blank
  }
}
