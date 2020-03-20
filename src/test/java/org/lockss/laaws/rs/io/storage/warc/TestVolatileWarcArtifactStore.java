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

import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link VolatileWarcArtifactDataStore}.
 */
public class TestVolatileWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<VolatileWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

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

  // *******************************************************************************************************************
  // * IMPLEMENTATION SPECIFIC TEST UTILITY METHODS
  // *******************************************************************************************************************

  protected URI expected_makeStorageUrl(ArtifactIdentifier aid, long offset, long length) throws Exception {
    return URI.create(String.format(
        "volatile://%s?offset=%d&length=%d",
        store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid()),
        offset,
        length
    ));
  }

  @Override
  protected Path[] expected_getBasePaths() {
    return new Path[]{VolatileWarcArtifactDataStore.DEFAULT_BASEPATH};
  }

  @Override
  protected Path[] expected_getTmpWarcBasePaths() {
    return new Path[]{expected_getBasePaths()[0].resolve(VolatileWarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH)};
  }

  @Override
  public void testInitDataStoreImpl() {
    assertEquals(WarcArtifactDataStore.DataStoreState.INITIALIZED, store.getDataStoreState());
  }

  @Override
  public void testInitCollectionImpl() throws Exception {
    // NOP
  }

  @Override
  public void testInitAuImpl() throws Exception {
    // NOP
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

  // *******************************************************************************************************************
  // * AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    ArtifactIdentifier aid = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);

    URI expectedStorageUrl = expected_makeStorageUrl(aid, 1234L, 5678L);

    Path activeWarcPath = store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid());
    URI actualStorageUrl = store.makeWarcRecordStorageUrl(activeWarcPath, 1234L, 5678L);

    assertEquals(expectedStorageUrl, actualStorageUrl);
  }

//  @Override
//  public void testGetInputStreamAndSeekImpl() throws Exception {
//  }

//  @Override
//  public void testGetAppendableOutputStreamImpl() throws Exception {
//  }

  @Override
  public void testInitWarcImpl() throws Exception {

  }

  @Override
  public void testGetWarcLengthImpl() throws Exception {

  }

  @Override
  public void testFindWarcsImpl() throws Exception {

  }

  @Override
  public void testRemoveWarcImpl() throws Exception {

  }

  @Override
  public void testGetBlockSizeImpl() throws Exception {

  }

  @Override
  public void testGetFreeSpaceImpl() throws Exception {

  }

  @Test
  public void testGetFreeSpace() throws Exception {
    Path randomPath = Paths.get(UUID.randomUUID().toString());

    // Setup spy of Runtime object
    Runtime s_runtime = spy(Runtime.getRuntime());
    when(s_runtime.freeMemory()).thenReturn(1234L);

    // Assert freeMemory() is called by getFreeSpace()
    assertEquals(s_runtime.freeMemory(), store.getFreeSpace(s_runtime, randomPath));

    // Assert valid freeMemory() output
    assertTrue(store.getFreeSpace(randomPath) > 0 && store.getFreeSpace(randomPath) <= s_runtime.maxMemory());
  }
}
