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

package org.lockss.laaws.rs.io.storage.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.AbstractWarcArtifactDataStoreTest;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestHdfsWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<HdfsWarcArtifactDataStore> {
    private final static L4JLogger log = L4JLogger.getLogger();

    private static MiniDFSCluster hdfsCluster;
    private String testRepoBasePath;

    @BeforeAll
    private static void startMiniDFSCluster() throws IOException {
//        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);

        File baseDir = new File("target/test-hdfs/" + UUID.randomUUID());
        baseDir.mkdirs();

        Configuration conf = new HdfsConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

        log.info(
            "Starting MiniDFSCluster with {} = {}, getBaseDirectory(): {}",
            MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
            conf.get(MiniDFSCluster.HDFS_MINIDFS_BASEDIR),
            MiniDFSCluster.getBaseDirectory()
        );

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
//        builder.numDataNodes(3);
//        builder.clusterId("test");

        hdfsCluster = builder.build();
        hdfsCluster.waitClusterUp();
    }

    @AfterAll
    private static void stopMiniDFSCluster() {
      if (hdfsCluster != null) {
        hdfsCluster.shutdown(true);
      }
    }

    @Override
    protected HdfsWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
        testRepoBasePath = String.format("/tests/%s", UUID.randomUUID());

        log.info("Creating HDFS artifact data store [baseDir: {}]", testRepoBasePath);

        assertNotNull(hdfsCluster);
      return new HdfsWarcArtifactDataStore(index, hdfsCluster.getFileSystem(), Paths.get(testRepoBasePath));
    }

    @Override
    protected HdfsWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index, HdfsWarcArtifactDataStore other) throws IOException {
      return new HdfsWarcArtifactDataStore(index, hdfsCluster.getFileSystem(), other.getBasePaths()[0]);
    }

    @Override
    protected java.nio.file.Path[] expected_getTmpWarcBasePaths() {
      List<java.nio.file.Path> paths = Arrays.stream(store.getBasePaths())
          .map(p -> p.resolve(WarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH))
          .collect(Collectors.toList());

      return Arrays.copyOf(paths.toArray(), paths.toArray().length, java.nio.file.Path[].class);
    }

    @Override
    public void testInitDataStoreImpl() throws Exception {
      assertTrue(isDirectory(store.getBasePaths()[0]));
      assertEquals(WarcArtifactDataStore.DataStoreState.INITIALIZED, store.getDataStoreState());
    }

  @Override
  public void testInitCollectionImpl() throws Exception {
    // Initialize a collection
    store.initCollection("collection");

    // Assert directory structures were created
    assertTrue(isAllDirectory(store.getCollectionPaths("collection")));
  }

  @Override
  public void testInitAuImpl() throws Exception {
    // Initialize an AU
    store.initAu("collection", "auid");

    // Assert directory structures were created
    assertTrue(isAllDirectory(store.getCollectionPaths("collection")));
    assertTrue(isAllDirectory(store.getAuPaths("collection", "auid")));
  }

  public boolean isAllDirectory(java.nio.file.Path[] paths) throws IOException {
    boolean allDirectory = true;

    for (java.nio.file.Path path : paths) {
      allDirectory &= isDirectory(Paths.get(path.toUri().getPath()));
    }

    return allDirectory;
    }

    @Override
    protected boolean pathExists(java.nio.file.Path path) throws IOException {
      log.debug("path = {}", path);
      return store.fs.exists(new Path(path.toString()));
    }

    @Override
    protected boolean isDirectory(java.nio.file.Path path) throws IOException {
        log.debug("path = {}", path);
      return store.fs.isDirectory(new Path(path.toString()));
    }

    @Override
    protected boolean isFile(java.nio.file.Path path) throws IOException {

      Path file = new Path(path.toString());

        if (!store.fs.exists(file)) {
            String errMsg = String.format("%s does not exist!", file);
            log.warn(errMsg);
        }

        return store.fs.isFile(file);
    }

  protected URI expected_makeStorageUrl(ArtifactIdentifier aid, long offset, long length) throws Exception {
    return URI.create(String.format("%s%s?offset=%d&length=%d",
        store.fs.getUri(),
        store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid()),
        offset,
        length
    ));
  }

  @Override
  protected java.nio.file.Path[] expected_getBasePaths() throws Exception {
    return new java.nio.file.Path[]{Paths.get(testRepoBasePath)};
  }


  // *******************************************************************************************************************
  // * AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    ArtifactIdentifier aid = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);

    URI expectedStorageUrl = expected_makeStorageUrl(aid, 1234L, 5678L);

    java.nio.file.Path activeWarcPath = store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid());
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

}
