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
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link WarcFilePool}.
 */
class TestWarcFilePool extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  @Test
  public void testCreateWarcFile() throws Exception {
    WarcFile warcFile;

    WarcArtifactDataStore mockedStore = mock(WarcArtifactDataStore.class);
    when(mockedStore.getBlockSize()).thenReturn(4096L);
    when(mockedStore.getThresholdWarcSize()).thenReturn(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);
    when(mockedStore.getBasePaths()).thenReturn(new Path[]{Paths.get("/lockss")});

    WarcFilePool pool = spy(new WarcFilePool(mockedStore));

//    // Assert that an IllegalStateException is thrown if the data store returns null array of temporary WARC dirs
//    when(mockedStore.getTmpWarcBasePaths()).thenReturn(null);
//    assertThrows(IllegalStateException.class, () -> pool.createWarcFile());
//
//    // Assert that an IllegalStateException is thrown if the data store returns an empty array of temporary WARC dirs
//    when(mockedStore.getTmpWarcBasePaths()).thenReturn(new Path[]{});
//    assertThrows(IllegalStateException.class, () -> pool.createWarcFile());
//
//    // Assert addWarc() is not called
//    verify(pool, never()).addWarcFile(org.mockito.ArgumentMatchers.any(WarcFile.class));
//
//    // Setup temporary WARC base paths
//    Path tmpWarcBasePath1 = Paths.get("/tmp1");
//    Path tmpWarcBasePath2 = Paths.get("/tmp2");
//    when(mockedStore.getFreeSpace(tmpWarcBasePath1)).thenReturn(0L);
//    when(mockedStore.getFreeSpace(tmpWarcBasePath2)).thenReturn(1L);
//
//    // Assert we get back a WarcFile with one temporary WARC dir
//    when(mockedStore.getTmpWarcBasePaths()).thenReturn(new Path[]{tmpWarcBasePath1});
//    warcFile = pool.createWarcFile();
//    assertNotNull(warcFile);
//    assertTrue(warcFile.getPath().startsWith(tmpWarcBasePath1));
//    assertEquals(0L, warcFile.getLength());
//    verify(pool).addWarcFile(warcFile);
//
//    // Assert we get back the expected WarcFile with two temporary WARC dirs
//    when(mockedStore.getTmpWarcBasePaths()).thenReturn(new Path[]{tmpWarcBasePath1, tmpWarcBasePath2});
//    warcFile = pool.createWarcFile();
//    assertNotNull(warcFile);
//    assertTrue(warcFile.getPath().startsWith(tmpWarcBasePath2));
//    assertEquals(0L, warcFile.getLength());
//    verify(pool).addWarcFile(warcFile);
//
//    // Assert we get back the expected WarcFile when tmpWarcBasePath1 suddenly has more space than tmpWarcBasePath2
//    when(mockedStore.getFreeSpace(tmpWarcBasePath1)).thenReturn(2L);
//    warcFile = pool.createWarcFile();
//    assertNotNull(warcFile);
//    assertTrue(warcFile.getPath().startsWith(tmpWarcBasePath1));
//    assertEquals(0L, warcFile.getLength());
//    verify(pool).addWarcFile(warcFile);
  }

  @Test
  public void testGenerateTmpWarcFileName() throws Exception {
    WarcFilePool pool = new WarcFilePool(null);
    String tmpWarcFileName = pool.generateTmpWarcFileName();

    // Assert generated file name is not null and ends with the WARC extension
    assertNotNull(tmpWarcFileName);
    assertTrue(tmpWarcFileName.endsWith(WarcArtifactDataStore.WARC_FILE_EXTENSION));
  }

  @Test
  public void testFindWarcFile() throws Exception {
    WarcFile warcFile1;

    WarcArtifactDataStore mockedStore = mock(WarcArtifactDataStore.class);
    when(mockedStore.getBlockSize()).thenReturn(4096L);
    when(mockedStore.getThresholdWarcSize()).thenReturn(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);

    Path tmpWarcPath = Paths.get("/tmp");
    when(mockedStore.getTmpWarcBasePaths()).thenReturn(new Path[]{tmpWarcPath});

    WarcFilePool pool = spy(new WarcFilePool(mockedStore));

    // Assert a new temporary WARC is created if there are no temporary WARCs in the pool
    warcFile1 = pool.findWarcFile(tmpWarcPath, 0L);
    verify(pool).createWarcFile(tmpWarcPath);
    assertTrue(pool.isInPool(warcFile1));
    assertTrue(pool.isInUse(warcFile1));

    // Return WarcFile to pool
    pool.returnWarcFile(warcFile1);
    assertTrue(pool.isInPool(warcFile1));
    assertFalse(pool.isInUse(warcFile1));

    // Assert we get back the same WarcFile since it is available again
    assertEquals(warcFile1, pool.findWarcFile(tmpWarcPath, 0L));

    // Assert the next call results in a different WarcFile since the first WARC is in use
    WarcFile warcFile2 = pool.findWarcFile(tmpWarcPath, 0L);
    assertNotEquals(warcFile1, warcFile2);

    // Return both WarcFiles
    pool.returnWarcFile(warcFile1);
    pool.returnWarcFile(warcFile2);

    // Assert that if the available WarcFiles don't have enough space, a new WarcFile is created
    WarcFile warcFile3 = pool.findWarcFile(tmpWarcPath, WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE + 1);
    assertNotEquals(warcFile1, warcFile3);
    assertNotEquals(warcFile2, warcFile3);

    // Assert findWarcFile() returns the WarcFile whose last block would be maximally filled by adding a record
    warcFile2.setLength(1234L);
    WarcFile warcFile4 = pool.findWarcFile(tmpWarcPath, 1000L);
    assertEquals(warcFile2, warcFile4);
  }

  @Test
  public void testBorrowWarcFile() throws Exception {
    WarcFilePool pool = spy(new WarcFilePool(null));

    WarcFile warcFile = mock(WarcFile.class);
    when(warcFile.getPath()).thenReturn(Paths.get("/tmp/foo.warc"));

    pool.addWarcFile(warcFile);

    // Assert borrowing the file succeeds
    assertNotNull(pool.borrowWarcFile(warcFile));

    // Assert borrowing the file again fails
    assertNull(pool.borrowWarcFile(warcFile));

    InOrder inOrder = Mockito.inOrder(pool);
    inOrder.verify(pool).borrowWarcFile(warcFile);
    inOrder.verify(pool).isInUse(warcFile);
  }

  @Test
  public void testReturnWarcFile() throws Exception {
    WarcFilePool pool = spy(new WarcFilePool(null));

    WarcFile warcFile = mock(WarcFile.class);
    when(warcFile.getPath()).thenReturn(Paths.get("/tmp/foo.warc"));

    // Verify adding an unknown WarcFile to the pool causes it to be added to the pool
    pool.returnWarcFile(warcFile);
    InOrder inOrder = Mockito.inOrder(pool);
    inOrder.verify(pool).isInPool(warcFile);
    inOrder.verify(pool).addWarcFile(warcFile);

    // Assert no changes returning a WarcFile already not in use
    pool.returnWarcFile(warcFile);
    assertTrue(pool.isInPool(warcFile));
    assertFalse(pool.isInUse(warcFile));

    // Borrow the file
    pool.borrowWarcFile(warcFile);

    // Assert WarcFile is now in use
    assertTrue(pool.isInPool(warcFile));
    assertTrue(pool.isInUse(warcFile));

    // Return the WarcFile
    pool.returnWarcFile(warcFile);
    assertTrue(pool.isInPool(warcFile));
    assertFalse(pool.isInUse(warcFile));
  }

  @Test
  public void testRemoveWarcFile() throws Exception {
    WarcFilePool pool = spy(new WarcFilePool(null));

    // Add a mock WarcFile to the pool and borrow it
    WarcFile warcFile = mock(WarcFile.class);
    pool.addWarcFile(warcFile);
    pool.borrowWarcFile(warcFile);

    // Assert WarcFile is part of the pool and in use
    assertTrue(pool.isInPool(warcFile));
    assertTrue(pool.isInUse(warcFile));

    // Remove WarcFile from pool
    pool.removeWarcFile(warcFile);

    // Assert WarcFile is removed
    assertFalse(pool.isInPool(warcFile));
    assertFalse(pool.isInUse(warcFile));
  }
}