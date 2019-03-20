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

import java.util.*;

public class WarcFilePool {
  private static final L4JLogger log = L4JLogger.getLogger();

  public static final long DEFAULT_THRESHOLD = FileUtils.ONE_GB;
  public static final long DEFAULT_BLOCKSIZE = 128 * FileUtils.ONE_MB;

  private final String poolBasePath;
  private final long blocksize;
  private final long sizeThreshold;

  private final Set<WarcFile> allWarcs = new HashSet<>();
  private final Set<WarcFile> usedWarcs = new HashSet<>();

  public WarcFilePool(String poolBasePath) {
    this(poolBasePath, DEFAULT_BLOCKSIZE, DEFAULT_THRESHOLD);
  }

  public WarcFilePool(String poolBasePath, long blocksize, long sizeThreshold) {
    this.poolBasePath = poolBasePath;
    this.blocksize = blocksize;
    this.sizeThreshold = sizeThreshold;
  }

  /**
   * Creates a new {@code WarcFile} and adds it to this pool.
   *
   * @return The new {@code WarcFile} instance.
   */
  protected WarcFile createWarcFile() {
    WarcFile warcFile = new WarcFile(poolBasePath + "/" + UUID.randomUUID().toString() + ".warc", 0);
    addWarcFile(warcFile);
    return warcFile;
  }

  /**
   * Adds one or more {@code WarcFile} objects to this pool.
   *
   * @param warcFile One or more {@code WarcFile} objects to add to this pool.
   */
  public void addWarcFile(WarcFile... warcFile) {
    synchronized (allWarcs) {
      allWarcs.addAll(Arrays.asList(warcFile));
    }
  }

  /**
   * Gets a suitable {@code WarcFile} for the number of bytes pending to be written, or creates one if one could not be
   * found.
   *
   * @param bytesExpected A {@code long} representing the number of bytes expected to be written.
   * @return A {@code WarcFile} from this pool.
   */
  public WarcFile getWarcFile(long bytesExpected) {
    if (bytesExpected < 0) {
      throw new IllegalArgumentException("bytesExpected must be a positive integer");
    }

    synchronized (allWarcs) {
      // Build set of available WARCs
      Set<WarcFile> availableWarcs = new HashSet<>();
      availableWarcs.addAll(allWarcs);

      synchronized (usedWarcs) {
        availableWarcs.removeAll(usedWarcs);

        Optional<WarcFile> opt = availableWarcs.stream()
            .filter(warc -> warc.getLength() + bytesExpected <= sizeThreshold)
            .max((w1, w2) ->
                (int) (
                    getBytesUsedLastBlock(w2.getLength() + bytesExpected) -
                    getBytesUsedLastBlock(w1.getLength() + bytesExpected)
                )
            );

        // Create a new WARC if no WarcFiles are available that can hold the expected number of bytes
        WarcFile warcFile = opt.isPresent() ? opt.get() : createWarcFile();

        // Add this WarcFile to the set of WarcFiles currently in use
        usedWarcs.add(warcFile);

        // Mark the WARC file as in use
        TempWarcInUseTracker.INSTANCE.markUseStart(warcFile.getPath());

        return warcFile;
      }
    }
  }

  /**
   * Computes the bytes used in the last block, assuming all previous blocks are maximally filled.
   *
   * @param size
   * @return
   */
  protected long getBytesUsedLastBlock(long size) {
    return ((size - 1) % blocksize) + 1;
  }

  /**
   * Makes an existing WarcFile available to this pool.
   *
   * @param warcFile
   *          The {@code WarcFile} to add back to this pool.
   */
  public void returnWarcFile(WarcFile warcFile) {
    synchronized (allWarcs) {
      if (isInPool(warcFile)) {
        synchronized (usedWarcs) {
          if (isInUse(warcFile)) {
            TempWarcInUseTracker.INSTANCE.markUseEnd(warcFile.getPath());
            usedWarcs.remove(warcFile);
          }
        }
      } else {
        log.warn("WARC file is not a member of this pool; adding it [warcFile: {}]", warcFile);
        addWarcFile(warcFile);
      }
    }
  }

  /**
   * Checks whether a {@code WarcFile} object is in this pool but in use by another thread.
   *
   * @param warcFile The {@code WarcFile} to check.
   * @return A {@code boolean} indicating whether the {@code WarcFile} is in use.
   */
  public boolean isInUse(WarcFile warcFile) {
    synchronized (usedWarcs) {
      return usedWarcs.contains(warcFile);
    }
  }

  /**
   * Checks whether a WARC file at a given path is a member of this pool but in use by another thread.
   *
   * @param warcFilePath A {@code String} containing the path to a {@code WarcFile} object in this pool.
   * @return A {@code boolean} indicating whether the {@code WarcFile} is in use.
   */
  public boolean isInUse(String warcFilePath) {
    synchronized (allWarcs) {
      WarcFile warcFile = lookupWarcFile(warcFilePath);
      return isInUse(warcFile);
    }
  }

  /**
   * Checks whether a {@code WarcFile} object is a member of this pool.
   *
   * @param warcFile The {@code WarcFile} to check.
   * @return A {@code boolean} indicating whether the {@code WarcFile} is a member of this pool.
   */
  public boolean isInPool(WarcFile warcFile) {
    synchronized (allWarcs) {
      return allWarcs.contains(warcFile);
    }
  }

  /**
   * Checks whether a WARC file at a given path is a member of this pool.
   *
   * @param warcFilePath A {@code String} containing the path to a {@code WarcFile} object in this pool.
   * @return A {@code boolean} indicating whether the {@code WarcFile} is a member of this pool.
   */
  public boolean isInPool(String warcFilePath) {
    synchronized (allWarcs) {
      WarcFile warcFile = lookupWarcFile(warcFilePath);
      return isInPool(warcFile);
    }
  }

  /**
   * Search for the WarcFile object in this pool that matches the given path. Returns {@code null} if one could not be
   * found.
   *
   * @param warcFilePath A {@code String} containing the path to the {@code WarcFile} to find.
   * @return The {@code WarcFile}, or {@code null} if one could not be found.
   */
  private WarcFile lookupWarcFile(String warcFilePath) {
    synchronized (allWarcs) {
      return allWarcs.stream()
          .filter(x -> x.getPath().equals(warcFilePath))
          .findAny()
          .orElse(null);
    }
  }

  /**
   * Removes the {@code WarcFile} matching the given path from this pool and returns it.
   *
   * May return {@code null} if none in the pool match.
   *
   * @param warcFilePath A {@code String} containing the WARC file path of the {@code WarcFile} to remove.
   * @return The {@code WarcFile} removed from this pool. May be {@code null} if not found.
   */
  public WarcFile removeWarcFile(String warcFilePath) {
    synchronized (allWarcs) {
      WarcFile warcFile = lookupWarcFile(warcFilePath);

      // If we found the WarcFile; remove it from the pool
      if (warcFile != null) {
        removeWarcFile(warcFile);
      }

      // Return the WarcFile that was found and removed, or return null
      return warcFile;
    }
  }

  /**
   * Removes an existing WarcFile from this pool.
   *
   * @param warcFile
   *          The instance of {@code WarcFile} to remove from this pool.
   */
  protected void removeWarcFile(WarcFile warcFile) {
    synchronized (allWarcs) {
      synchronized (usedWarcs) {
        if (usedWarcs.contains(warcFile)) {
          usedWarcs.remove(warcFile);
        }
      }

      if (allWarcs.contains(warcFile)) {
        allWarcs.remove(warcFile);
      }
    }
  }

  /**
   * Dumps a snapshot of all {@code WarcFile} objects in this pool.
   */
  public void dumpWarcFilesPoolInfo() {
    long totalBlocksAllocated = 0;
    long totalBytesUsed = 0;
    long numWarcFiles = 0;

    // Iterate over WarcFiles in this pool
    synchronized (allWarcs) {
      for (WarcFile warc : allWarcs) {
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
