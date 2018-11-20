/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.io.FileUtils;
import org.lockss.log.L4JLogger;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class WarcFilePool {
  private static final L4JLogger log = L4JLogger.getLogger();

  public static final long DEFAULT_THRESHOLD = FileUtils.ONE_GB;
  public static final long DEFAULT_BLOCKSIZE = 128 * FileUtils.ONE_MB;

  private final String poolBasePath;
  private final long blocksize;
  private final long sizeThreshold;

  private final Set<WarcFile> warcFiles = new HashSet<>();

  public WarcFilePool(String poolBasePath) {
    this(poolBasePath, DEFAULT_BLOCKSIZE, DEFAULT_THRESHOLD);
  }

  public WarcFilePool(String poolBasePath, long blocksize, long sizeThreshold) {
    this.poolBasePath = poolBasePath;
    this.blocksize = blocksize;
    this.sizeThreshold = sizeThreshold;
  }

  /**
   * Finds a suitable WarcFile for the number of bytes pending to be written, or creates one if one could not be found.
   *
   * @param bytesExpected
   * @return
   */
  public synchronized WarcFile findWarcFile(long bytesExpected) {
    // TODO: An argument check on bytesExpected

    Optional<WarcFile> opt = warcFiles.stream()
        .filter(warc -> warc.getLength() + bytesExpected < sizeThreshold)
        // TODO: Use stream().max()
        .sorted((w1, w2) ->
            // TODO: Write a method, unit test, and comment explain this:
            (int)((((w2.getLength() - 1 + bytesExpected) % blocksize) + 1) - (((w1.getLength() - 1 + bytesExpected) % blocksize) + 1)))
        .findFirst();

    WarcFile warcFile;

    if (opt.isPresent()) {
      warcFile = opt.get();
    } else {
      warcFile = createWarcFile();
    }

    removeWarcFile(warcFile);

    return warcFile;
  }

  /**
   * Creates a new WarcFile at the base path of this WarcFile pool.
   *
   * @return A new {@code WarcFile} instance
   */
  protected synchronized WarcFile createWarcFile() {
    WarcFile warcFile = new WarcFile(poolBasePath + "/" + UUID.randomUUID().toString() + ".warc", 0);
    warcFiles.add(warcFile);
    return warcFile;
  }

  /**
   * Makes an existing WarcFile available to this pool.
   *
   * @param warcFile
   *          The {@code WarcFile} to add back to this pool.
   */
  public synchronized void returnWarcFile(WarcFile warcFile) {
    warcFiles.add(warcFile);
  }

  /**
   * Removes an existing WarcFile from this pool.
   *
   * @param warcFile
   *          The instance of {@code WarcFile} to remove from this pool.
   */
  protected synchronized void removeWarcFile(WarcFile warcFile) {
    warcFiles.remove(warcFile);
  }

  /**
   * Dumps a snapshot of all WarcFiles in this pool.
   */
  public synchronized void dumpWarcFilesPoolInfo() {
    long totalBlocksAllocated = 0;
    long totalBytesUsed = 0;
    long numWarcFiles = 0;

    // Iterate over WarcFiles in this pool
    for (WarcFile warc : warcFiles) {
      long blocks = (long) Math.ceil(new Float(warc.getLength()) / new Float(blocksize));
      totalBlocksAllocated += blocks;
      totalBytesUsed += warc.getLength();

      // Log information per WarcFile
      log.info(String.format(
          "WARC: %s [Length: %d, Blocks: %d]",
          warc.getPath(),
          warc.getLength(),
          blocks
      ));

      numWarcFiles++;
    }

    // Log aggregate information about the pool of WarcFiles
    log.info(String.format(
        "Summary: %d bytes allocated (%d blocks) using %d bytes (%.2f%%) in %d WARC files",
        totalBlocksAllocated * blocksize,
        totalBlocksAllocated,
        totalBytesUsed,
        100.0f * new Float(totalBytesUsed) / new Float(totalBlocksAllocated * blocksize),
        numWarcFiles
    ));
  }
}
