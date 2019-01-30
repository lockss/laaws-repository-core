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
import org.junit.jupiter.api.*;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.log.L4JLogger;

import java.io.*;

/**
 * Test class for {org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore}.
 */
public class TestVolatileWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<VolatileWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore() throws IOException {
    return new VolatileWarcArtifactDataStore();
  }

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore(VolatileWarcArtifactDataStore other) throws IOException {
    VolatileWarcArtifactDataStore n_store = new VolatileWarcArtifactDataStore();
    n_store.warcs.putAll(other.warcs);
    return n_store;
  }

  @Override
  protected void runTestGetTmpWarcBasePath() {
    assertNotNull(store.getTmpWarcBasePath());
    assertEquals("/tmp", store.getTmpWarcBasePath());
  }

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
  protected boolean pathExists(String path) {
    return isFile(path) || isDirectory(path);
  }

  @Override
  protected boolean isDirectory(String path) {
    return store.warcs.keySet().stream().anyMatch(x -> x.startsWith(path + "/"));
  }

  @Override
  protected boolean isFile(String path) {
    return store.warcs.get(path) != null;
  }

  @Override
  protected String getAbsolutePath(String path) {
    return path;
  }

  @Override
  protected String testMakeStorageUrl_getExpected(ArtifactIdentifier ident, long offset) throws Exception {
    return String.format(
        "volatile://%s%s?offset=%d",
        (store.getBasePath().equals("/") ? "" : store.getBasePath()),
        store.getActiveWarcPath(ident),
        offset
    );
  }

  @Override
  protected boolean isValidStorageUrl(String storageUrl) {
    return true;
  }
}
