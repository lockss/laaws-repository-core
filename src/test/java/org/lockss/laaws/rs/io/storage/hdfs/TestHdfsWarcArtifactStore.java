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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.lockss.laaws.rs.core.BaseLockssRepository;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.AbstractWarcArtifactDataStoreTest;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;
import org.mockito.ArgumentMatchers;
import org.springframework.util.MultiValueMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link org.lockss.laaws.rs.io.storage.hdfs.HdfsWarcArtifactDataStore}, the Apache Hadoop Distributed
 * File System (HDFS) based implementation of {@link WarcArtifactDataStore}.
 */
public class TestHdfsWarcArtifactStore extends AbstractWarcArtifactDataStoreTest<HdfsWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static MiniDFSCluster hdfsCluster;
  private java.nio.file.Path basePath;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  private final static class ProxyLockssTestCase5 extends LockssTestCase5 {
    // Intentionally left blank
  }

  private final static ProxyLockssTestCase5 proxy = new ProxyLockssTestCase5();

  @BeforeAll
  public static void startMiniDFSCluster() throws IOException {
    // Get temporary directory for HDFS data dir
    File dataDir = proxy.getTempDir();

    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());

    log.info("Starting MiniDFSCluster");

    // Build MiniDFSCluster using custom HDFS configuration
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    hdfsCluster = builder.build();

    // Wait for cluster to start
    hdfsCluster.waitClusterUp();
  }

  @AfterAll
  public static void stopMiniDFSCluster() {
    if (hdfsCluster != null) {
      hdfsCluster.shutdown(true);
    }
  }

  @Override
  protected HdfsWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
    basePath = Paths.get("/lockss/test").resolve(UUID.randomUUID().toString());

    log.info("Creating HDFS artifact data store [basePath: {}]", basePath);

    assertNotNull(hdfsCluster);

    HdfsWarcArtifactDataStore ds =
        new HdfsWarcArtifactDataStore(hdfsCluster.getFileSystem(), basePath);

    // Mock getArtifactIndex() called by data store
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    ds.setLockssRepository(repo);

    return ds;
  }

  @Override
  protected HdfsWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index, HdfsWarcArtifactDataStore other)
      throws IOException {

    HdfsWarcArtifactDataStore ds =
        new HdfsWarcArtifactDataStore(hdfsCluster.getFileSystem(), other.getBasePaths()[0]);

    // Mock getArtifactIndex() called by data store
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    ds.setLockssRepository(repo);

    return ds;
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION-SPECIFIC TEST UTILITY METHODS
  // *******************************************************************************************************************

  @Override
  protected boolean pathExists(java.nio.file.Path path) throws IOException {
    log.debug("path = {}", path);
    return store.fs.exists(new org.apache.hadoop.fs.Path(path.toString()));
  }

  @Override
  protected boolean isDirectory(java.nio.file.Path path) throws IOException {
    log.debug("path = {}", path);
    return store.fs.getFileStatus(new org.apache.hadoop.fs.Path(path.toString())).isDirectory();
  }

  @Override
  protected boolean isFile(java.nio.file.Path path) throws IOException {

    org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(path.toString());

    if (!store.fs.exists(file)) {
      String errMsg = String.format("%s does not exist!", file);
      log.warn(errMsg);
    }

    return store.fs.isFile(file);
  }

  @Override
  protected java.nio.file.Path[] expected_getBasePaths() throws Exception {
    return new java.nio.file.Path[]{basePath};
  }

  @Override
  protected java.nio.file.Path[] expected_getTmpWarcBasePaths() {
    List<java.nio.file.Path> paths = Arrays.stream(store.getBasePaths())
        .map(p -> p.resolve(WarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH))
        .collect(Collectors.toList());

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, java.nio.file.Path[].class);
  }

  // *******************************************************************************************************************
  // * TEST: Constructors
  // *******************************************************************************************************************

  // TODO

  // *******************************************************************************************************************
  // * TEST: AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  /**
   * Test for {@link HdfsWarcArtifactDataStore#init()}.
   *
   * @throws Exception
   */
  @Override
  public void testInitDataStoreImpl() throws Exception {
    // Mocks
    WarcArtifactDataStore ds = mock(WarcArtifactDataStore.class);

    // Mock behavior
    doCallRealMethod().when(ds).init();

    // Initialize data store
    ds.init();

    // FIXME: Method access not permissive enough (protected) - why?
//    verify(ds).reloadDataStoreState();

    assertNotEquals(WarcArtifactDataStore.DataStoreState.STOPPED, store.getDataStoreState());
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#initNamespace(String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitNamespaceImpl() throws Exception {
    String namespace = "ns1";
    final java.nio.file.Path[] nsPaths = new java.nio.file.Path[]{Paths.get("/a"), Paths.get("/b")};

    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);

    // Mock behavior
    doCallRealMethod().when(ds).initNamespace(ArgumentMatchers.any());
    when(ds.getNamespacePaths(namespace)).thenReturn(nsPaths);

    // Assert bad input results in IllegalArgumentException being thrown
    assertThrows(IllegalArgumentException.class, () -> ds.initNamespace(null));
    assertThrows(IllegalArgumentException.class, () -> ds.initNamespace(""));

    // Initialize a namespace
    ds.initNamespace(namespace);

    // Assert expected directory structures were created
    verify(ds).mkdirs(nsPaths);
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#initAu(String, String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitAuImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    ds.fs = mock(FileSystem.class);
    FileStatus status = mock(FileStatus.class);
    Path basePath = mock(Path.class);
    Path auPath1 = mock(Path.class, "/lockss/test");
    Path auPath2 = mock(Path.class);

    // Mock behavior
    doCallRealMethod().when(ds).clearAuMaps();
    doCallRealMethod().when(ds).initAu(NS1, AUID1);

    // Assert IllegalStateException thrown if no base paths configured in data store
    when(ds.getBasePaths()).thenReturn(null);
    assertThrows(IllegalStateException.class, () -> ds.initAu(NS1, AUID1));

    // Assert IllegalStateException thrown if empty base paths
    when(ds.getBasePaths()).thenReturn(new Path[]{});
    assertThrows(IllegalStateException.class, () -> ds.initAu(NS1, AUID1));

    // FIXME: Initialize maps
//    FieldSetter.setField(ds, ds.getClass().getDeclaredField("auPathsMap"), new HashMap<>());
//    FieldSetter.setField(ds, ds.getClass().getDeclaredField("auActiveWarcsMap"), new HashMap<>());
    ds.clearAuMaps();

    // Assert IOException causes data store attempt to re-create directory
    when(ds.getBasePaths()).thenReturn(new Path[]{basePath});
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath1);
    when(ds.fs.getFileStatus(ArgumentMatchers.any())).thenThrow(IOException.class);
    when(ds.initAuDir(NS1, AUID1)).thenReturn(auPath2);
    assertTrue(ds.initAu(NS1, AUID1).contains(auPath2));
    verify(ds).initAuDir(NS1, AUID1);
    clearInvocations(ds);
    reset(ds.fs);

    when(ds.fs.getFileStatus(ArgumentMatchers.any())).thenReturn(status);

    // Assert if no AU paths found then a new one is created
    when(ds.getBasePaths()).thenReturn(new Path[]{basePath});
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath1);
    when(status.isDirectory()).thenReturn(false);
    when(ds.initAuDir(NS1, AUID1)).thenReturn(auPath2);
    assertTrue(ds.initAu(NS1, AUID1).contains(auPath2));
    verify(ds).initAuDir(NS1, AUID1);
    clearInvocations(ds);

    // Assert if existing AU paths are found on disk then they are just returned
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath1);
    List<Path> auPaths = new ArrayList<>();
    auPaths.add(auPath1);
    when(status.isDirectory()).thenReturn(true);
    assertIterableEquals(auPaths, ds.initAu(NS1, AUID1));
    verify(ds, never()).initAuDir(NS1, AUID1);
    clearInvocations(ds);
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#makeStorageUrl(java.nio.file.Path, MultiValueMap)}.
   *
   * @throws Exception
   */
  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    // Q: Does it make sense to write a test for this? What would we exercise?
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#initWarc(java.nio.file.Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitWarcImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    Path warcPath = mock(Path.class);
    ds.fs = mock(FileSystem.class);
    OutputStream output = mock(OutputStream.class);

    // Mock behavior
    doCallRealMethod().when(ds).initWarc(warcPath);
    doCallRealMethod().when(ds).initFile(warcPath);
    when(ds.getAppendableOutputStream(warcPath)).thenReturn(output);
    when(warcPath.toString()).thenReturn("test");

    // Call method
    ds.initWarc(warcPath);

    // Verify file is created then an WARC info record is written to it
    verify(ds.fs).createNewFile(ArgumentMatchers.any());
    verify(ds).getAppendableOutputStream(warcPath);
    verify(ds).writeWarcInfoRecord(output);
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#getWarcLength(java.nio.file.Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testGetWarcLengthImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    Path warcPath = Paths.get("/lockss/test.warc");
    ds.fs = mock(FileSystem.class);

    // Mock behavior
    doCallRealMethod().when(ds).getWarcLength(ArgumentMatchers.any(Path.class));

    // Assert that it returns 0 if file doesn't exist
    when(ds.fs.getFileStatus(ArgumentMatchers.any())).thenThrow(FileNotFoundException.class);
    assertEquals(0L, ds.getWarcLength(warcPath));

    // Reset FileSystem mock (so that it no longer throws)
    reset(ds.fs);

    // Assert we get the result of FileStatus#getLen()
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getLen()).thenReturn(1234L);
    when(ds.fs.getFileStatus(ArgumentMatchers.any())).thenReturn(fileStatus);
    assertEquals(1234L, ds.getWarcLength(warcPath));
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#findWarcs(java.nio.file.Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testFindWarcsImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    ds.fs = mock(FileSystem.class);
    Path basePath = mock(Path.class);
    FileStatus fsBasePathStatus = mock(FileStatus.class);

    // Mock behavior
    doCallRealMethod().when(ds).findWarcs(ArgumentMatchers.any(Path.class));
    when(basePath.toString()).thenReturn("/lockss");
    when(ds.fs.getFileStatus(ArgumentMatchers.any())).thenReturn(fsBasePathStatus);

    // Assert IllegalStateException thrown if base path exists but is not a directory
    when(ds.fs.exists(ArgumentMatchers.any())).thenReturn(true);
    when(fsBasePathStatus.isDirectory()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> ds.findWarcs(basePath));

    // Assert empty set is returned if base path does not exist
    when(ds.fs.exists(ArgumentMatchers.any())).thenReturn(false);
    when(fsBasePathStatus.isDirectory()).thenReturn(false);
    assertEmpty(ds.findWarcs(basePath));

    // List of file names
    String[] filenames = new String[]{
        "foo",
        "bar.warc.gz",
        "bar.warc",
        "xyzzy.txt"
    };

    // Build an Iterator<LocatedFileStatus> containing mocked LocatedFileStatus of the file names
    Iterator<LocatedFileStatus> inner = Arrays.stream(filenames).map(filename -> {
      LocatedFileStatus status = mock(LocatedFileStatus.class);

      // Set mocked isFile() return
      when(status.isFile()).thenReturn(true);

      // Set mocked getPath() return
      org.apache.hadoop.fs.Path mockedPath = mock(org.apache.hadoop.fs.Path.class);
      when(mockedPath.getName()).thenReturn(filename);
      when(mockedPath.toUri()).thenReturn(URI.create(filename));
      when(status.getPath()).thenReturn(mockedPath);

      return status;
    }).iterator();

    // Wrap Iterator into a RemoteIterator
    RemoteIterator<LocatedFileStatus> files = new WrappedIteratorRemoteIterator<>(inner);

    // Assert only WARCs returned
    when(ds.fs.listFiles(ArgumentMatchers.any(), eq(true))).thenReturn(files);
    when(ds.fs.exists(ArgumentMatchers.any())).thenReturn(true);
    when(fsBasePathStatus.isDirectory()).thenReturn(true);
    Collection<Path> result = ds.findWarcs(basePath);

    assertEquals(2, result.size());
    // assertTrue(result.contains(ListUtil.list(Paths.get("bar.warc.gz"), Paths.get("bar.warc"))));
  }

  /**
   * Wraps an {@link Iterator} into a Apache Hadoop {@link RemoteIterator}.
   *
   * @param <T> the type of elements returned by this iterator
   */
  class WrappedIteratorRemoteIterator<T> implements RemoteIterator<T> {
    Iterator<T> inner;

    public WrappedIteratorRemoteIterator(Iterator<T> inner) {
      this.inner = inner;
    }

    @Override
    public boolean hasNext() {
      return inner.hasNext();
    }

    @Override
    public T next() {
      return inner.next();
    }
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#removeWarc(java.nio.file.Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testRemoveWarcImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    ds.fs = mock(FileSystem.class);
    Path mockPath = mock(Path.class);

    // Mock behavior
    when(mockPath.toString()).thenReturn("test");
    doCallRealMethod().when(ds).removeWarc(ArgumentMatchers.any(Path.class));

    // Call method
    ds.removeWarc(mockPath);

    // Verify FileSystem#delete(Path) is called
    verify(ds.fs).delete(ArgumentMatchers.any(org.apache.hadoop.fs.Path.class), eq(false));
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#getBlockSize()}.
   *
   * @throws Exception
   */
  @Override
  public void testGetBlockSizeImpl() throws Exception {
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    doCallRealMethod().when(ds).getBlockSize();
    assertEquals(HdfsWarcArtifactDataStore.DEFAULT_BLOCKSIZE, ds.getBlockSize());
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#getFreeSpace(java.nio.file.Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testGetFreeSpaceImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    ds.fs = mock(FileSystem.class);
    Path mockPath = mock(Path.class);
    FsStatus fsStatus = mock(FsStatus.class);

    // Mock behavior
    when(mockPath.toString()).thenReturn("test");
    when(ds.fs.getStatus(ArgumentMatchers.any())).thenReturn(fsStatus);
    doCallRealMethod().when(ds).getFreeSpace(ArgumentMatchers.any(Path.class));

    // Call method
    ds.getFreeSpace(mockPath);

    // Verify FsStatus#getRemaining() called
    verify(ds.fs).getStatus(ArgumentMatchers.any());
    verify(fsStatus).getRemaining();
  }

  /**
   * Test for {@link HdfsWarcArtifactDataStore#initAuDir(String, String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitAuDirImpl() throws Exception {
    // Mocks
    HdfsWarcArtifactDataStore ds = mock(HdfsWarcArtifactDataStore.class);
    ds.fs = mock(FileSystem.class);
    Path basePath = mock(Path.class);
    Path auPath = mock(Path.class);
    org.apache.hadoop.fs.Path hdfsAuPath = mock(org.apache.hadoop.fs.Path.class);
    FileStatus status = mock(FileStatus.class);

    // Mock behavior
    doCallRealMethod().when(ds).initAuDir(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath);
    when(ds.hdfsPathFromPath(auPath)).thenReturn(hdfsAuPath);
    when(ds.fs.getFileStatus(hdfsAuPath)).thenReturn(status);

    // Assert IllegalStateException thrown if getBasePaths() returns null or is empty
    when(ds.getBasePaths()).thenReturn(null);
    assertThrows(IllegalStateException.class, () -> ds.initAuDir(NS1, AUID1));
    when(ds.getBasePaths()).thenReturn(new Path[]{});
    assertThrows(IllegalStateException.class, () -> ds.initAuDir(NS1, AUID1));

    when(ds.getBasePaths()).thenReturn(new Path[]{basePath});

    // Assert directory created if not directory
    when(status.isDirectory()).thenReturn(false);
    assertEquals(auPath, ds.initAuDir(NS1, AUID1));
    verify(ds).mkdirs(auPath);
    clearInvocations(ds);

    // Assert directory is *not* created if directory
    when(ds.fs.exists(hdfsAuPath)).thenReturn(true);
    when(status.isDirectory()).thenReturn(true);
    assertEquals(auPath, ds.initAuDir(NS1, AUID1));
    verify(ds, never()).mkdirs(auPath);
    clearInvocations(ds);
  }
}
