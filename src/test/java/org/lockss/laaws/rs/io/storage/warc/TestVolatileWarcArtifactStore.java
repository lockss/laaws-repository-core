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
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Test class for {org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore}.
 */
public class TestVolatileWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<VolatileWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
    return new VolatileWarcArtifactDataStore(index);
  }

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index, VolatileWarcArtifactDataStore other) throws IOException {
    VolatileWarcArtifactDataStore n_store = new VolatileWarcArtifactDataStore(index);
    n_store.warcs.putAll(other.warcs);
    return n_store;
  }

  @Override
  protected Path[] expected_getBasePaths() {
    return new Path[]{Paths.get(VolatileWarcArtifactDataStore.DEFAULT_BASEPATH)};
  }

  @Override
  protected Path[] expected_getTmpWarcBasePaths() {
    return new Path[]{expected_getBasePaths()[0].resolve(VolatileWarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH)};
  }

  @Override
  public void runTestInitArtifactDataStore() {
    assertEquals(WarcArtifactDataStore.DataStoreState.INITIALIZED, store.getDataStoreState());
  }

  @Override
  public void runTestInitCollection() throws Exception {
    // NOP
  }

  @Override
  public void runTestInitAu() throws Exception {
    // NOP
  }

  @Override
  @Test
  public void testGetAuMetadataWarcPath() throws Exception {
    ArtifactIdentifier ident1 = new ArtifactIdentifier("coll1", "auid1", null, null);
    Path expectedAuDirPath = expected_getBasePaths()[0].resolve("collections/coll1/au-" + DigestUtils.md5Hex("auid1"));
    String expectedFileName = "lockss-repo.warc";
    Path expectedPath = expectedAuDirPath.resolve(expectedFileName);
    Path actualPath = store.getAuMetadataWarcPath(ident1, RepositoryArtifactMetadata.LOCKSS_METADATA_ID);
    assertEquals(expectedPath, actualPath);
  }

  @Override
  protected boolean pathExists(Path path) {
    return isFile(path) || isDirectory(path);
  }

  @Override
  protected boolean isDirectory(Path path) {
    return store.warcs.keySet().stream().anyMatch(x -> x.startsWith(path + "/"));
  }

  @Override
  protected boolean isFile(Path path) {
    return store.warcs.get(path) != null;
  }

  @Override
  protected String expected_makeStorageUrl(ArtifactIdentifier aid, long offset, long length) throws Exception {
    return String.format(
        "volatile://%s?offset=%d&length=%d",
        store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid()),
        offset,
        length
    );
  }
}
