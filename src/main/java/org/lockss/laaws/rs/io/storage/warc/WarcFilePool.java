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

import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class WarcFilePool {
  private static final L4JLogger log = L4JLogger.getLogger();

  protected WarcArtifactDataStore store;
  protected Set<WarcFile> allWarcs = new HashSet<>();
  protected Set<WarcFile> usedWarcs = new HashSet<>(); // TODO: Map from WarcFile to WarcFile's state (enum)

  public WarcFilePool(WarcArtifactDataStore store) {
    this.store = store;
  }

  /**
   * Creates a new {@link WarcFile} under the given path and adds it to this pool.
   *
   * @return The new {@link WarcFile} instance.
   */
  protected WarcFile createWarcFile(Path basePath) throws IOException {
    Path tmpWarcDir = basePath.resolve(WarcArtifactDataStore.TMP_WARCS_DIR);

    WarcFile warcFile =
        new WarcFile(tmpWarcDir.resolve(generateTmpWarcFileName()), store.getUseWarcCompression());

    store.initWarc(warcFile.getPath());

    addWarcFile(warcFile);

    return warcFile;
  }

  /**
   * Creates a new temporary WARC file under one of the temporary WARC directories configured
   * in the data store.
   */
  protected WarcFile createWarcFile() throws IOException {
    Path basePath = Arrays.stream(store.getBasePaths())
        .max((a, b) -> (int) (store.getFreeSpace(b) - store.getFreeSpace(a)))
        .orElse(null);

    return createWarcFile(basePath);
  }

  protected String generateTmpWarcFileName() {
    return UUID.randomUUID() + store.getWarcFileExtension();
  }

  /**
   * Adds one or more {@link WarcFile} objects to this pool.
   *
   * @param warcFile One or more {@link WarcFile} objects to add to this pool.
   */
  public void addWarcFile(WarcFile... warcFile) {
    synchronized (allWarcs) {
      allWarcs.addAll(Arrays.asList(warcFile));
    }
  }

  /**
   * Gets a suitable {@link WarcFile} for the number of bytes pending to be written, or creates one if one could not be
   * found.
   *
   * @param bytesExpected A {@code long} representing the number of bytes expected to be written.
   * @return A {@link WarcFile} from this pool.
   */
  public WarcFile findWarcFile(Path basePath, long bytesExpected) throws IOException {
    if (bytesExpected < 0) {
      throw new IllegalArgumentException("bytesExpected must be a positive integer");
    }

    synchronized (allWarcs) {
      // Build set of available WARCs
      Set<WarcFile> availableWarcs = new HashSet<>(allWarcs);

      synchronized (usedWarcs) {
        availableWarcs.removeAll(usedWarcs);

        Optional<WarcFile> opt = availableWarcs.stream()
            .filter(warc -> warc.getPath().startsWith(basePath))
            .filter(warc -> warc.isCompressed() == store.getUseWarcCompression())
            .filter(warc -> warc.getLength() + bytesExpected <= store.getThresholdWarcSize())
            .max((w1, w2) ->
                (int) (
                    getBytesUsedLastBlock(w1.getLength() + bytesExpected) -
                        getBytesUsedLastBlock(w2.getLength() + bytesExpected)
                )
            );

        // Create a new WARC if no WarcFiles are available that can hold the expected number of bytes
        WarcFile warcFile = opt.isPresent() ? opt.get() : createWarcFile(basePath);

        // Add this WarcFile to the set of WarcFiles currently in use
        usedWarcs.add(warcFile);

        // Mark the WARC file as in use
        TempWarcInUseTracker.INSTANCE.markUseStart(warcFile.getPath());

        return warcFile;
      }
    }
  }

  /**
   * Returns a WARC file from the pool (or first creates a new one if one is not currently available).
   */
  public WarcFile findWarcFile() throws IOException {
    synchronized (allWarcs) {
      // Build set of available WARCs
      Set<WarcFile> availableWarcs = new HashSet<>(allWarcs);

      synchronized (usedWarcs) {
        availableWarcs.removeAll(usedWarcs);

        Optional<WarcFile> optWarc = availableWarcs.stream()
            .filter(warc -> warc.getArtifacts() < store.getMaxArtifactsThreshold())
            .filter(warc -> warc.isCompressed() == store.getUseWarcCompression())
            .findAny();

        WarcFile warcFile = optWarc.isPresent() ?
            optWarc.get() : createWarcFile();

        usedWarcs.add(warcFile);
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
    return ((size - 1) % store.getBlockSize()) + 1;
  }

  /**
   * Makes an existing {@link WarcFile} available to this pool.
   *
   * @param warcFile The {@link WarcFile} to add back to this pool.
   */
  public void returnWarcFile(WarcFile warcFile) {
    boolean isSizeReached = warcFile.getLength() >= store.getThresholdWarcSize();
    boolean isArtifactsReached = warcFile.getArtifacts() >= store.getMaxArtifactsThreshold();
    boolean retireWarc = isSizeReached || isArtifactsReached;

    synchronized (allWarcs) {
      if (isInPool(warcFile)) {

        // Remove from internal set of used WARCs
        synchronized (usedWarcs) {
          if (isInUse(warcFile)) {
            TempWarcInUseTracker.INSTANCE.markUseEnd(warcFile.getPath());
            usedWarcs.remove(warcFile);
          } else {
            log.warn("WARC file is a member of this pool but was not in use [warcFile: {}]", warcFile);
          }
        }

        // Remove from pool if full
        if (retireWarc) allWarcs.remove(warcFile);

      } else if (!retireWarc) {
        // Add WARC file to this pool (for the first time?)
        log.warn("WARC file is not a member of this pool; adding it [warcFile: {}]", warcFile);
        addWarcFile(warcFile);
      }
    }
  }

  /**
   * Checks whether a {@link WarcFile} object is in this pool but in use by another thread.
   *
   * @param warcFile The {@link WarcFile} to check.
   * @return A {@code boolean} indicating whether the {@link WarcFile} is in use.
   */
  public boolean isInUse(WarcFile warcFile) {
    synchronized (usedWarcs) {
      return usedWarcs.contains(warcFile);
    }
  }

  /**
   * Checks whether a WARC file at a given path is a member of this pool but in use by another thread.
   *
   * @param warcFilePath A {@link String} containing the path to a {@link WarcFile} object in this pool.
   * @return A {@code boolean} indicating whether the {@link WarcFile} is in use.
   */
  public boolean isInUse(Path warcFilePath) {
    synchronized (allWarcs) {
      WarcFile warcFile = lookupWarcFile(warcFilePath);
      return isInUse(warcFile);
    }
  }

  /**
   * Checks whether a {@link WarcFile} object is a member of this pool.
   *
   * @param warcFile The {@link WarcFile} to check.
   * @return A {@code boolean} indicating whether the {@link WarcFile} is a member of this pool.
   */
  public boolean isInPool(WarcFile warcFile) {
    synchronized (allWarcs) {
      return allWarcs.contains(warcFile);
    }
  }

  /**
   * Checks whether a WARC file at a given path is a member of this pool.
   *
   * @param warcFilePath A {@link String} containing the path to a {@link WarcFile} object in this pool.
   * @return A {@code boolean} indicating whether the {@link WarcFile} is a member of this pool.
   */
  public boolean isInPool(Path warcFilePath) {
    synchronized (allWarcs) {
      WarcFile warcFile = lookupWarcFile(warcFilePath);
      return isInPool(warcFile);
    }
  }

  /**
   * Search for the WarcFile object in this pool that matches the given path. Returns {@code null} if one could not be
   * found.
   *
   * @param warcFilePath A {@link String} containing the path to the {@link WarcFile} to find.
   * @return The {@link WarcFile}, or {@code null} if one could not be found.
   */
  public WarcFile lookupWarcFile(Path warcFilePath) {
    synchronized (allWarcs) {
      return allWarcs.stream()
          .filter(x -> x.getPath().equals(warcFilePath))
          .findFirst()
          .orElse(null);
    }
  }

  /**
   * Removes the {@link WarcFile} matching the given path from this pool and returns it.
   * <p>
   * May return {@code null} if none in the pool match.
   *
   * @param warcFilePath A {@link String} containing the WARC file path of the {@link WarcFile} to remove.
   * @return The {@link WarcFile} removed from this pool. May be {@code null} if not found.
   */
  public WarcFile removeWarcFile(Path warcFilePath) {
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
   * Removes an existing {@link WarcFile} from this pool.
   *
   * @param warcFile The instance of {@link WarcFile} to remove from this pool.
   */
  protected void removeWarcFile(WarcFile warcFile) {
    synchronized (allWarcs) {
      synchronized (usedWarcs) {
        if (isInUse(warcFile)) {
          // Pay attention to this log message - it may indicate a problem with the code
          log.warn("Forceful removal of WARC file from pool [warcFile: {}]", warcFile);

          // Q: What should we do if it's currently in use?
        }

        usedWarcs.remove(warcFile);
      }

      allWarcs.remove(warcFile);
    }
  }

  /**
   * Dumps a snapshot of all {@link WarcFile} objects in this pool.
   */
  public void dumpWarcFilesPoolInfo() {
    long totalBlocksAllocated = 0;
    long totalBytesUsed = 0;
    long numWarcFiles = 0;

    // Iterate over WarcFiles in this pool
    synchronized (allWarcs) {
      for (WarcFile warcFile : allWarcs) {
        long blocks = (long) Math.ceil(new Float(warcFile.getLength()) / new Float(store.getBlockSize()));
        totalBlocksAllocated += blocks;
        totalBytesUsed += warcFile.getLength();

        // Log information per WarcFile
        log.debug2(
            "[path = {}, length = {}, blocks = {}, inUse = {}]",
            warcFile.getPath(),
            warcFile.getLength(),
            blocks,
            usedWarcs.contains(warcFile)
        );

        numWarcFiles++;
      }
    }

    // Log aggregate information about the pool of WarcFiles
    log.debug(String.format(
        "Summary: %d bytes allocated (%d blocks) using %d bytes (%.2f%%) in %d WARC files",
        totalBlocksAllocated * store.getBlockSize(),
        totalBlocksAllocated,
        totalBytesUsed,
        100.0f * new Float(totalBytesUsed) / new Float(totalBlocksAllocated * store.getBlockSize()),
        numWarcFiles
    ));
  }
}
