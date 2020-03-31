/*
 * Copyright (c) 2017-2020, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.springframework.util.FileSystemUtils;
import java.io.File;
import org.lockss.laaws.rs.model.*;
import org.lockss.util.storage.StorageInfo;

/**
 * Test class for {@code org.lockss.laaws.rs.core.LocalLockssRepository}
 */
public class TestLocalLockssRepositoryPersist extends AbstractLockssRepositoryTest {
    private final static L4JLogger log = L4JLogger.getLogger();

    // The local repository root directory.
    private File repoBaseDir = null;

    @Override
    public LockssRepository makeLockssRepository() throws Exception {
        repoBaseDir = getTempDir();
        return new LocalLockssRepository(repoBaseDir, "persist.ser");
    }

    /**
     * Run after the test is finished.
     */
    @AfterEach
    @Override
    public void tearDownArtifactDataStore() throws Exception {
        super.tearDownArtifactDataStore();

        // Clean up the local repository directory tree used in the test.
        log.info("Cleaning up local repository directory used for tests: {}", repoBaseDir);
        if (!FileSystemUtils.deleteRecursively(repoBaseDir)) {
          log.warn("Failed to delete temporary directory " + repoBaseDir);
        }
    }

  @Test
  public void testRepoInfo() throws Exception {
    RepositoryInfo ri = repository.getRepositoryInfo();
    log.debug("repoinfo: {}", ri);
    StorageInfo ind = ri.getIndexInfo();
    StorageInfo sto = ri.getStoreInfo();
    assertEquals("disk", ind.getType());
    assertTrue(ind.getSize() > 0);
    assertEquals("disk", sto.getType());
    assertTrue(sto.getSize() > 0);
    assertTrue(sto.isSameDevice(ind));
  }
}
