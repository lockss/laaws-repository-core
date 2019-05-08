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

package org.lockss.laaws.rs.core;

import java.io.*;

import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.LocalArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.local.LocalWarcArtifactDataStore;
import org.lockss.log.L4JLogger;

/**
 * Local filesystem implementation of the LOCKSS Repository API that uses a volatile artifact index.
 */
public class LocalLockssRepository extends BaseLockssRepository {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Takes a base path and persisted index name and instantiates a LocalLockssRepository.
   *
   * @param basePath
   *          A {@code File} containing the base path of this LOCKSS repository.
   * @param persistedIndexName
   *          A String with the name of the file where to persist the index.
   */
  public LocalLockssRepository(File basePath, String persistedIndexName) throws IOException {
    this.index = new LocalArtifactIndex(basePath, persistedIndexName);
    this.store = new LocalWarcArtifactDataStore(index, basePath);
  }

  protected LocalLockssRepository(ArtifactIndex ai, ArtifactDataStore ads) throws IOException {
    super(ai, ads); /*...*/
  }

  /**
   * Constructor that takes a local filesystem base path, and an instance of an ArtifactIndex implementation.
   *
   * @param index
   *          An {@code ArtifactIndex} to use as this repository's artifact index.
   * @param basePath
   *          A {@code File} containing the base path of this LOCKSS repository.
   */
  public LocalLockssRepository(ArtifactIndex index, File basePath) throws IOException {
    super(index, new LocalWarcArtifactDataStore(index, basePath));
  }
}