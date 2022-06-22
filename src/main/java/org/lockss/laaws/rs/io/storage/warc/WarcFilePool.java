/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.laaws.rs.io.storage.warc;

import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.log.L4JLogger;
import org.lockss.util.time.TimeBase;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
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
            .filter(warc -> warc.getArtifactsUncommitted() < store.getMaxArtifactsThreshold())
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
    boolean isArtifactsReached = warcFile.getArtifactsUncommitted() >= store.getMaxArtifactsThreshold();
    boolean closeWarcFile = isSizeReached || isArtifactsReached;

    synchronized (allWarcs) {
      if (closeWarcFile) {
        // Remove from this WARC file pool
        removeWarcFileFromPool(warcFile);
      } else if (!isInPool(warcFile)) {
        // Add WARC file to this pool (possibly for the first time)
        log.warn("WARC file was not yet a member of this pool [warcFile: {}]", warcFile);
        addWarcFile(warcFile);
      } else if (isInUse(warcFile)) {
        // Mark end-of-use of this WarcFile
        synchronized (usedWarcs) {
          TempWarcInUseTracker.INSTANCE.markUseEnd(warcFile.getPath());
          usedWarcs.remove(warcFile);
        }
      } else {
        // Nothing else to do
        log.warn("WARC file is a member but not marked in-use in this pool [warcFile: {}]", warcFile);
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
      WarcFile warcFile = getWarcFile(warcFilePath);
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
      return getWarcFile(warcFilePath) != null;
    }
  }

  /**
   * Search for the WarcFile object in this pool that matches the given path. Returns {@code null} if one could not be
   * found.
   *
   * @param warcFilePath A {@link String} containing the path to the {@link WarcFile} to find.
   * @return The {@link WarcFile}, or {@code null} if one could not be found.
   */
  private WarcFile getWarcFile(Path warcFilePath) {
    synchronized (allWarcs) {
      return allWarcs.stream()
          .filter(x -> x.getPath().equals(warcFilePath))
          .findFirst()
          .orElse(null);
    }
  }

  /**
   * Removes the {@link WarcFile} from this pool and returns it. May return {@code null} if there is no match of the
   * given WARC file path.
   *
   * @param warcFilePath A {@link Path} containing the WARC file path of the {@link WarcFile} to remove.
   * @return The {@link WarcFile} removed from this pool. May be {@code null} if not found.
   */
  public WarcFile removeWarcFileFromPool(Path warcFilePath) {
    synchronized (allWarcs) {
      WarcFile warcFile = getWarcFile(warcFilePath);

      // If we found the WarcFile; remove it from the pool
      if (warcFile != null) {
        removeWarcFileFromPool(warcFile);
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
  public void removeWarcFileFromPool(WarcFile warcFile) {
    synchronized (allWarcs) {
      synchronized (usedWarcs) {
        if (isInPool(warcFile) && isInUse(warcFile)) {
          log.warn("Forcefully removing WARC file from pool [warcFile: {}]", warcFile);
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
        100.0f * (float) totalBytesUsed / (float) (totalBlocksAllocated * store.getBlockSize()),
        numWarcFiles
    ));
  }

  public void runGC() {
    // WARCs to GC
    List<WarcFile> removedWarcs = new LinkedList<>();

    // Determine which WARCs to GC; remove from pool while synchronized
    synchronized (allWarcs) {
      for (WarcFile warc : allWarcs) {
        Instant now = Instant.ofEpochMilli(TimeBase.nowMs());
        Instant expiration = Instant.ofEpochMilli(warc.getLatestExpiration());

        int uncommitted = warc.getArtifactsUncommitted();
        int committed = warc.getArtifactsCommitted();
        int copied = warc.getArtifactsCopied();

        if (committed == copied && (uncommitted == 0 || now.isAfter(expiration))) {
          removeWarcFileFromPool(warc);
          removedWarcs.add(warc);
        }
      }
    }

    // GC removed WARCs from the index (if necessary) and data store
    for (WarcFile warc : removedWarcs) {
      // Remove index references if there are any uncommitted
      if (warc.getArtifactsUncommitted() != 0) {
        ArtifactIndex index = store.getArtifactIndex();

        try {
          // TODO: Determine journal path
          Path artifactStateJournal = Paths.get("XXX");

          store.readJournal(artifactStateJournal, ArtifactStateEntry.class)
              .stream()
              .filter(ArtifactStateEntry::isCopied)
              .map(ArtifactStateEntry::getArtifactId)
              .forEach(artifactId -> {
                try {
                  index.deleteArtifact(artifactId);
                } catch (IOException e) {
                  log.error("Could not remove index reference [artifactId: " + artifactId + ", warc: " + warc + "]", e);
                  // TODO: Do not leave the index in an inconsistent state!
                  throw new RuntimeException(e);
                }
              });
        } catch (IOException e) {
          log.error("Error reading journal [journal: " + warc.getPath() + "]", e);
          return;
        }
      }

      // Remove WARC from the data store
      try {
        store.removeWarc(warc.getPath());
      } catch (IOException e) {
        // Log error and leave to reload
        log.error("Could not remove WARC file " + warc.getPath(), e);
      }
    }
  }
}
