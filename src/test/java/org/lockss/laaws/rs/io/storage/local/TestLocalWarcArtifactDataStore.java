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

package org.lockss.laaws.rs.io.storage.local;

import org.apache.commons.io.FilenameUtils;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.warc.AbstractWarcArtifactDataStoreTest;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link LocalWarcArtifactDataStore}, the local filesystem based implementation of
 * {@link WarcArtifactDataStore}.
 */
public class TestLocalWarcArtifactDataStore extends AbstractWarcArtifactDataStoreTest<LocalWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();
  private File testRepoBasePath;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
    testRepoBasePath = getTempDir();
    testRepoBasePath.mkdirs();

    return new LocalWarcArtifactDataStore(index, new File[]{testRepoBasePath});
  }

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index, LocalWarcArtifactDataStore other)
      throws IOException {

    return new LocalWarcArtifactDataStore(index, other.getBasePaths());
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION-SPECIFIC TEST UTILITY METHODS
  // *******************************************************************************************************************

  @Override
  protected boolean pathExists(Path path) throws IOException {
    return path.toFile().exists();
  }

  @Override
  protected boolean isDirectory(Path path) {
    return path.toFile().isDirectory();
  }

  @Override
  protected boolean isFile(Path path) {
    return path.toFile().isFile();
  }

  // *******************************************************************************************************************

  @Override
  protected Path[] expected_getTmpWarcBasePaths() {
    return new Path[]{testRepoBasePath.toPath().resolve(WarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH)};
  }

  @Override
  protected Path[] expected_getBasePaths() {
    return new Path[]{testRepoBasePath.toPath()};
  }

  // *******************************************************************************************************************
  // * TEST: Constructors
  // *******************************************************************************************************************

//  @Test
//  public void testLocalWarcArtifactDataStoreConstructor() throws Exception {
//  }

  // *******************************************************************************************************************
  // * TEST: AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    ArtifactIdentifier aid = new ArtifactIdentifier("coll1", "auid1", "http://example.com/u1", 1);

    URI expectedStorageUrl = URI.create(String.format(
        "file://%s?offset=%d&length=%d",
        store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid()),
        1234L,
        5678L
    ));

    Path activeWarcPath = store.getAuActiveWarcPath(aid.getCollection(), aid.getAuid());
    URI actualStorageUrl = store.makeWarcRecordStorageUrl(activeWarcPath, 1234L, 5678L);

    assertEquals(expectedStorageUrl, actualStorageUrl);
  }

  @Override
  public void testInitWarcImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedWarcPath = mock(Path.class);
    File mockedWarcFile = mock(File.class);

    // Mock behavior
    when(mockedWarcPath.toFile()).thenReturn(mockedWarcFile);
    doCallRealMethod().when(ds).initWarc(mockedWarcPath);

    // Assert a new WARC is not initialized if the WARC already exists
    when(mockedWarcFile.exists()).thenReturn(true);
    ds.initWarc(mockedWarcPath);
    verify(ds, never()).initFile(mockedWarcFile);
    verify(ds, never()).getAppendableOutputStream(mockedWarcPath);
    verify(ds, never()).writeWarcInfoRecord(ArgumentMatchers.any(OutputStream.class));

    // Assert a new WARC is initialized otherwise
    when(mockedWarcFile.exists()).thenReturn(false);
    ds.initWarc(mockedWarcPath);
    verify(ds, times(1)).initFile(mockedWarcFile);
    verify(ds, times(1)).getAppendableOutputStream(mockedWarcPath);
    verify(ds, times(1)).writeWarcInfoRecord(ArgumentMatchers.any(/* OutputStream.class */)); // FIXME
  }

  @Override
  public void testGetWarcLengthImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    doCallRealMethod().when(ds).getWarcLength(mockedPath);

    // Assert length() is called on Path.toFile()
    ds.getWarcLength(mockedPath);
    verify(mockedFile, times(1)).length();
  }

  @Override
  public void testFindWarcsImpl() throws Exception {
    // Mocks
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Connect mocked File to mocked Path
    when(mockedPath.toFile()).thenReturn(mockedFile);

    // Assert findWarcs() returns empty set if path does not exist
    when(mockedFile.exists()).thenReturn(false);
    assertEmpty(store.findWarcs(mockedPath));

    // Assert findWarcs() returns empty set if path exists but is not a directory
    when(mockedFile.exists()).thenReturn(true);
    when(mockedFile.isDirectory()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> store.findWarcs(mockedPath));

    // Trigger an IOException because of an IOException in listFiles()
    when(mockedFile.exists()).thenReturn(true);
    when(mockedFile.isDirectory()).thenReturn(true);
    when(mockedFile.listFiles()).thenReturn(null);
    assertThrows(IOException.class, () -> store.findWarcs(mockedPath));

    // Setup to trigger a recursion of findWarcs()
    File mockedFileDir = mockFile(true, true, "test");
    when(mockedFile.listFiles()).thenReturn(new File[]{
        mockedFileDir
    });

    // Verify recursion of findWarcs()
    LocalWarcArtifactDataStore ds = spy(store);
    ds.findWarcs(mockedPath);
    verify(ds).findWarcs(mockedFileDir.toPath());

    // Setup WARC file discovery for current directory
    File[] mockedFiles = new File[]{
        mockFile(true, false, "test"),
        mockFile(true, false, "test1"),
        mockFile(true, false, "test.warc"),
        mockFile(true, false, "test2.warc"),
    };

    when(mockedFile.listFiles()).thenReturn(mockedFiles);

    Collection<Path> paths = store.findWarcs(mockedPath);

    log.trace("paths = {}", paths);

    // Assert findWarcs() returns only WARCs
    assertTrue(paths.stream().map(Path::toString)
        .allMatch(name -> FilenameUtils.getExtension(name).equalsIgnoreCase(WarcArtifactDataStore.WARC_FILE_EXTENSION))
    );
  }

  private File mockFile(boolean exists, boolean isDir, String name) {
    File mockedFile = mock(File.class);

    when(mockedFile.exists()).thenReturn(exists);
    when(mockedFile.isFile()).thenReturn(!isDir);
    when(mockedFile.isDirectory()).thenReturn(isDir);
    when(mockedFile.getName()).thenReturn(name);
    when(mockedFile.toPath()).thenReturn(Paths.get(name));

    return mockedFile;
  }

  @Override
  public void testRemoveWarcImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    doCallRealMethod().when(ds).removeWarc(mockedPath);

    // Assert delete() is called on Path.toFile()
    ds.removeWarc(mockedPath);
    verify(mockedFile, times(1)).delete();
  }

  @Override
  public void testGetBlockSizeImpl() throws Exception {
    assertEquals(LocalWarcArtifactDataStore.DEFAULT_BLOCKSIZE, store.getBlockSize());
  }

  @Override
  public void testGetFreeSpaceImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    doCallRealMethod().when(ds).getFreeSpace(mockedPath);

    // Assert getFreeSpace() is called on Path.toFile()
    ds.getFreeSpace(mockedPath);
    verify(mockedFile, times(1)).getFreeSpace();
  }

  @Override
  public void testInitDataStoreImpl() throws Exception {
    assertTrue(Arrays.stream(store.getBasePaths())
        .map(this::isDirectory)
        .allMatch(Predicate.isEqual(true)));

    assertEquals(WarcArtifactDataStore.DataStoreState.INITIALIZED, store.getDataStoreState());
  }

  @Override
  public void testInitCollectionImpl() throws Exception {
    final String collectionId = "collection";
    final Path[] collectionPaths = new Path[]{Paths.get("/a"), Paths.get("/b")};

    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);

    // Mock behavior
    when(ds.getCollectionPaths(collectionId)).thenReturn(collectionPaths);

    // Initialize a collection
    doCallRealMethod().when(ds).initCollection(collectionId);
    ds.initCollection(collectionId);

    // Assert directory structures were created
    verify(ds).mkdirs(collectionPaths);
  }

  @Override
  public void testInitAuImpl() throws Exception {
    final String collectionId = "collection";
    final String auid = "auid";
    final Path[] auPaths = new Path[]{Paths.get("/a"), Paths.get("/b")};

    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    when(ds.getAuPaths(collectionId, auid)).thenReturn(auPaths);

    // Initialize the AU
    doCallRealMethod().when(ds).initAu(collectionId, auid);
    ds.initAu(collectionId, auid);

    // Verify correct directory structures were created
    for (Path auPath : auPaths) {
      verify(ds).mkdirs(auPath);
//      verify(ds).mkdirs(auPath.resolve("artifacts"));
//      verify(ds).mkdirs(auPath.resolve("journals"));
    }
  }
}