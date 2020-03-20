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

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.util.anvl.Element;
import org.json.JSONObject;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.CloseCallbackInputStream;
import org.lockss.util.concurrent.stripedexecutor.StripedCallable;
import org.lockss.util.concurrent.stripedexecutor.StripedExecutorService;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.time.TimeUtil;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This abstract class aims to capture operations that are common to all {@link ArtifactDataStore} implementations that
 * serialize {@link ArtifactData} as WARC records in a WARC file.
 */
public abstract class WarcArtifactDataStore implements ArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata>, WARCConstants {
  private final static L4JLogger log = L4JLogger.getLogger();

  // DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS") does not parse in Java 8: https://bugs.openjdk.java.net/browse/JDK-8031085
  protected static final DateTimeFormatter FMT_TIMESTAMP =
      new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss")
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withZone(ZoneId.of("UTC"));

  public static final String WARC_FILE_EXTENSION = "warc";
  protected static final String AU_DIR_PREFIX = "au-";

  protected static final String COLLECTIONS_DIR = "collections";
  protected static final String TMP_WARCS_DIR = "temp";

  protected static final String WARCID_SCHEME = "urn:uuid";
  protected static final String CRLF = "\r\n";
  protected static byte[] CRLF_BYTES;

  public static final Path DEFAULT_BASEPATH = Paths.get("/lockss");
  public final static String DEFAULT_TMPWARCBASEPATH = TMP_WARCS_DIR;

  private static final long DEFAULT_DFOS_THRESHOLD = 16L * FileUtils.ONE_MB;

  protected static final String ENV_THRESHOLD_WARC_SIZE = "REPO_MAX_WARC_SIZE";
  protected static final long DEFAULT_THRESHOLD_WARC_SIZE = 1L * FileUtils.ONE_GB;
  protected long thresholdWarcSize;

  protected static final String ENV_UNCOMMITTED_ARTIFACT_EXPIRATION = "REPO_UNCOMMITTED_ARTIFACT_EXPIRATION";
  protected static final long DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION = TimeUtil.WEEK;
  protected long uncommittedArtifactExpiration;

  protected ArtifactIndex artifactIndex;
  public static Path[] basePaths;
  protected WarcFilePool tmpWarcPool;
  protected Map<CollectionAuidPair, List<Path>> auActiveWarcsMap = new HashMap<>();
  protected DataStoreState dataStoreState = DataStoreState.UNINITIALIZED;

  protected ScheduledExecutorService scheduledExecutor;
  protected StripedExecutorService stripedExecutor;

  // The Artifact life cycle states.
  protected enum ArtifactState {
    UNKNOWN,
    NOT_INDEXED,
    UNCOMMITTED,
    EXPIRED,
    COMMITTED,
    COPIED,
    DELETED
  }

  public enum DataStoreState {
    UNINITIALIZED,
    INITIALIZED,
    SHUTDOWN
  }

  // *******************************************************************************************************************
  // * STATIC
  // *******************************************************************************************************************

  static {
    try {
      CRLF_BYTES = CRLF.getBytes(DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  // *******************************************************************************************************************
  // * ABSTRACT METHODS
  // *******************************************************************************************************************

  protected abstract URI makeStorageUrl(Path filePath, MultiValueMap<String, String> params);

  protected abstract InputStream getInputStreamAndSeek(Path filePath, long seek) throws IOException;

  protected abstract OutputStream getAppendableOutputStream(Path filePath) throws IOException;

  protected abstract void initWarc(Path warcPath) throws IOException;

  protected abstract long getWarcLength(Path warcPath) throws IOException;

  protected abstract Collection<Path> findWarcs(Path basePath) throws IOException;

  protected abstract boolean removeWarc(Path warcPath) throws IOException;

  protected abstract long getBlockSize();

  protected abstract long getFreeSpace(Path fsPath);

  // *******************************************************************************************************************
  // * CONSTRUCTORS
  // *******************************************************************************************************************

  /**
   * Base constructor for {@link WarcArtifactDataStore} implementations.
   */
  public WarcArtifactDataStore(ArtifactIndex index) {
    // Start executors
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    this.stripedExecutor = new StripedExecutorService();

    // Set ArtifactIndex to use
    setArtifactIndex(index);

    // Set WARC threshold size to use
    setThresholdWarcSize(NumberUtils.toLong(System.getenv(ENV_THRESHOLD_WARC_SIZE), DEFAULT_THRESHOLD_WARC_SIZE));

    // Set uncommitted artifact expiration interval
    setUncommittedArtifactExpiration(
        NumberUtils.toLong(System.getenv(ENV_UNCOMMITTED_ARTIFACT_EXPIRATION), DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION)
    );
  }

  // *******************************************************************************************************************
  // * DATA STORE LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Initializes the data store.
   */
  @Override
  public void initDataStore() {
    log.debug2("Initializing data store");

    reloadDataStoreState();
    setDataStoreState(DataStoreState.INITIALIZED);

    log.info("Initialized data store");
  }

  /**
   * Shutdowns the data store.
   *
   * @throws InterruptedException
   */
  @Override
  public void shutdownDataStore() throws InterruptedException {
    if (dataStoreState != DataStoreState.SHUTDOWN) {
      scheduledExecutor.shutdown();
      stripedExecutor.shutdown();

      // TODO: Parameterize
      scheduledExecutor.awaitTermination(1, TimeUnit.MINUTES);
      stripedExecutor.awaitTermination(1, TimeUnit.MINUTES);

      setDataStoreState(DataStoreState.SHUTDOWN);

      log.info("Finished shutdown of data store");
    } else {
      log.info("Data store is already shutdown");
    }
  }

  /**
   * Returns the state of this data store.
   *
   * @return A {@link DataStoreState} indicating the state of this data store.
   */
  public DataStoreState getDataStoreState() {
    return dataStoreState;
  }

  /**
   * Sets the state of this data store.
   *
   * @param state The new {@link DataStoreState} state of this data store.
   */
  protected void setDataStoreState(DataStoreState state) {
    this.dataStoreState = state;
  }

  /**
   * Schedules an asynchronous reload of the data store state.
   */
  protected void reloadDataStoreState() {
    stripedExecutor.submit(new ReloadDataStoreStateTask());
  }

  private class ReloadDataStoreStateTask implements Runnable {
    @Override
    public void run() {

      try {
        // Reload temporary WARCs
        reloadTemporaryWarcs();

        // TODO: Reload active WARCs
        // reloadActiveWarcs();

        // Schedule temporary WARC garbage collection
        // TODO: Parameterize interval
        scheduledExecutor.scheduleAtFixedRate(new GarbageCollectTempWarcsTask(), 0, 1, TimeUnit.DAYS);

        log.info("Scheduled temporary WARC garbage collector");

      } catch (Exception e) {
        log.error("Could not reload data store state: {}", e);
        throw new IllegalStateException(e);
      }
    }
  }

  // *******************************************************************************************************************
  // * INTERNAL PATH METHODS
  // *******************************************************************************************************************

  /**
   * Experimental. Returns the base of a path encoded in a storage URL.
   *
   * @param storageUrl
   * @return
   * @throws URISyntaxException
   */
  private Path getBasePathFromStorageUrl(URI storageUrl) throws URISyntaxException {
    Path warcPath = Paths.get(storageUrl.getPath());

    return Arrays.stream(getBasePaths())
        .filter(basePath -> warcPath.startsWith(basePath.toString()))
        .sorted(Path::compareTo)
        .findFirst()
        .get();
  }

  /**
   * Returns a {@code boolean} indicating whether a {@link Path} is under a temporary WARC base directory.
   *
   * @param path The {@link Path} to check.
   * @return A {@code boolean} indicating whether the {@link Path} is under a temporary WARC base directory.
   */
  protected boolean isTmpStorage(Path path) {
    return Arrays.stream(getTmpWarcBasePaths())
        .map(basePath -> path.startsWith(basePath))
        .anyMatch(Predicate.isEqual(true));
  }

  /**
   * Returns the base paths configured in this data store.
   *
   * @return A {@link Path[]} containing the base paths of this data store.
   */
  public Path[] getBasePaths() {
    return basePaths;
  }

  /**
   * Returns an array containing all the temporary WARC base paths (one for each base path of this data store).
   *
   * @return A {@link Path[]} containing all the temporary WARC base paths of this data store.
   */
  protected Path[] getTmpWarcBasePaths() {
    Path[] basePaths = getBasePaths();

    if (basePaths == null) {
      throw new IllegalStateException("No base paths configured in data store!");
    }

    return Arrays.stream(basePaths)
        .map(basePath -> basePath.resolve(TMP_WARCS_DIR))
        .toArray(Path[]::new);
  }

  @Deprecated
  public Path[] getCollectionTmpWarcsPaths(String collectionId) {
    return Arrays.stream(getCollectionPaths(collectionId))
        .map(path -> path.resolve(TMP_WARCS_DIR))
        .toArray(Path[]::new);
  }

  /**
   * Returns the collections base path, given a base path of this data store.
   *
   * @param basePath A {@link Path} containing a base path of this data store.
   * @return A {@link Path} containing the collections base path, under the given data store base path.
   */
  public Path getCollectionsBasePath(Path basePath) {
    return basePath.resolve(COLLECTIONS_DIR);
  }

  /**
   * Returns an array containing all the collections base paths (one for each base path of this data store).
   *
   * @return A {@link Path[]} containing all the collections base paths of this data store.
   */
  public Path[] getCollectionsBasePaths() {
    return Arrays.stream(getBasePaths())
        .map(path -> getCollectionsBasePath(path))
        .toArray(Path[]::new);
  }

  /**
   * Returns the base path of a collection, given its name and a base path of this data store.
   *
   * @param basePath     A {@link Path} containing a base path of this data store.
   * @param collectionId A {@link String} containing the name of the collection.
   * @return A {@link Path} containing the base path of the collection, under the given data store base path.
   */
  public Path getCollectionPath(Path basePath, String collectionId) {
    return getCollectionsBasePath(basePath).resolve(collectionId);
  }

  /**
   * Returns an array containing all the paths of this collection (one for each base path of this data store).
   *
   * @param collectionId A {@link String} containing the name of the collection.
   * @return A {@link Path[]} containing all paths of this collection.
   */
  public Path[] getCollectionPaths(String collectionId) {
    return Arrays.stream(getBasePaths())
        .map(path -> getCollectionPath(path, collectionId))
        .toArray(Path[]::new);
  }

  /**
   * Returns the base path of an AU, given its AUID, the collection ID it belongs to, and a base path of the data store.
   *
   * @param basePath     A {@link Path} containing a base path of this data store.
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link Path} containing the base path of the AU, under the given data store base path.
   */
  public Path getAuPath(Path basePath, String collectionId, String auid) {
    return getCollectionPath(basePath, collectionId).resolve(AU_DIR_PREFIX + DigestUtils.md5Hex(auid));
  }

  /**
   * Returns an array containing all the paths of this AU (one for each base path of this data store).
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link Path[]} containing all paths of this AU.
   */
  public Path[] getAuPaths(String collectionId, String auid) {
    return Arrays.stream(getBasePaths())
        .map(path -> getAuPath(path, collectionId, auid))
        .toArray(Path[]::new);
  }

  /**
   * Returns an active WARC of an AU, on the base path having the most free space.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link Path} containing the active WARC
   * @throws IOException
   */
  public Path getAuActiveWarcPath(String collectionId, String auid) throws IOException {
    synchronized (auActiveWarcsMap) {
      Path[] activeWarcs = getAuActiveWarcPaths(collectionId, auid);

      // TODO: Prioritize starting an active WARC on a base path that does not have an active WARC
      if (activeWarcs.length < getBasePaths().length) {
        return initAuActiveWarc(collectionId, auid);
      }

      Path activeWarc = Arrays.stream(activeWarcs)
          .sorted((a, b) -> (int) (getFreeSpace(b.getParent()) - getFreeSpace(a.getParent())))
          .findFirst()
          .orElse(null);

      return activeWarc == null ? initAuActiveWarc(collectionId, auid) : activeWarc;
    }
  }

  /**
   * Returns an array containing all the active WARCs of this AU.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link Path[]} containing all active WARCS of this AU.
   */
  public Path[] getAuActiveWarcPaths(String collectionId, String auid) {
    // Key into the active WARC map
    CollectionAuidPair aukey = new CollectionAuidPair(collectionId, auid);

    synchronized (auActiveWarcsMap) {
      List<Path> paths = auActiveWarcsMap.get(aukey);
      return paths == null ? new Path[]{} : paths.toArray(new Path[0]);
    }
  }

  /**
   * Returns the path of a journal of an AU on the given base path.
   *
   * @param basePath    A {@link Path} containing a repository base path.
   * @param aid         An {@link ArtifactIdentifier} containing the artifact identifier.
   * @param journalName A {@link String} containing the name of the journal.
   * @return A {@link Path} containing the path to the journal.
   */
  protected Path getAuMetadataWarcPath(Path basePath, ArtifactIdentifier aid, String journalName) {
    return getAuPath(basePath, aid.getCollection(), aid.getAuid()).resolve(journalName + "." + WARC_FILE_EXTENSION);
  }

  /**
   * Returns the path to a journal of an AU across all the configured data store base paths.
   *
   * @param aid         An {@link ArtifactIdentifier} containing the artifact identifier.
   * @param journalName A {@link String} containing the name of the journal.
   * @return A {@link Path[]} containing the paths to the journal on across all the configured data store base paths.
   */
  protected Path[] getAuMetadataWarcPaths(ArtifactIdentifier aid, String journalName) {
    return Arrays.stream(getAuPaths(aid.getCollection(), aid.getAuid()))
        .map(path -> path.resolve(journalName + "." + WARC_FILE_EXTENSION))
        .toArray(Path[]::new);
  }

  // *******************************************************************************************************************
  // * INTERNAL STORAGE URL
  // *******************************************************************************************************************

  /**
   * Convenience method that encodes the location, offset, and length of a WARC record into an internal storage URL.
   *
   * @param filePath A {@link Path} containing the path to the WARC file containing the WARC record.
   * @param offset   A {@code long} containing the byte offset from the beginning of this WARC file to the beginning of
   *                 the WARC record.
   * @param length   A {@code long} containing the length of the WARC record.
   * @return A {@link URI} internal storage URL encoding the location, offset, and length of the WARC record.
   */
  protected URI makeWarcRecordStorageUrl(Path filePath, long offset, long length) {
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add("offset", Long.toString(offset));
    params.add("length", Long.toString(length));
    return makeStorageUrl(filePath, params);
  }

  /**
   * Convenience method that encodes a {@link WarcRecordLocation} into an internal storage URL.
   *
   * @param recordLocation
   * @return A {@link URI} internal storage URL encoding the location, offset, and length of the {@link WarcRecordLocation}.
   */
  protected URI makeWarcRecordStorageUrl(WarcRecordLocation recordLocation) {
    return makeWarcRecordStorageUrl(
        recordLocation.getPath(),
        recordLocation.getOffset(),
        recordLocation.getLength()
    );
  }

  /**
   * Returns the path component of a storage URL.
   *
   * @param storageUrl A {@link URI} containing the storage URL.
   * @return A {@link Path} containing the path component of the storage URL.
   */
  @Deprecated
  public static Path getPathFromStorageUrl(URI storageUrl) {
    return Paths.get(storageUrl.getPath());
  }

  // *******************************************************************************************************************
  // * METHODS
  // *******************************************************************************************************************

  /**
   * Marks the file as in-use and returns an {@link InputStream} to the beginning of the file.
   *
   * @param filePath A {@link Path} containing the path to the file.
   * @return An {@link InputStream} to the file.
   * @throws IOException
   */
  protected InputStream markAndGetInputStream(Path filePath) throws IOException {
    return markAndGetInputStreamAndSeek(filePath, 0L);
  }

  /**
   * Marks the file as in-use and returns an {@link InputStream} to the file, after seeking by {@code offset} bytes.
   *
   * @param filePath A {@link Path} containing the path to the file.
   * @param offset   A {@code long} containing the number of bytes to seek.
   * @return An {@link InputStream} to the file.
   * @throws IOException
   */
  protected InputStream markAndGetInputStreamAndSeek(Path filePath, long offset) throws IOException {
    TempWarcInUseTracker.INSTANCE.markUseStart(filePath);

    InputStream warcStream = new BufferedInputStream(getInputStreamAndSeek(filePath, offset));

    return new CloseCallbackInputStream(
        warcStream,
        closingWarcFile -> {
          // Decrement the counter of times that the file is in use.
          TempWarcInUseTracker.INSTANCE.markUseEnd((Path) closingWarcFile);
        },
        filePath
    );
  }

  // *******************************************************************************************************************
  // * AU ACTIVE WARCS LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Generates a file name for a new active WARC of an AU. Makes no guarantee about file name uniqueness.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link String} containing the generated active WARC file name.
   */
  protected String generateActiveWarcName(String collectionId, String auid) {
    String timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(FMT_TIMESTAMP);
    String auidHash = DigestUtils.md5Hex(auid);
    return String.format("artifacts_%s-%s_%s.warc", collectionId, auidHash, timestamp);
  }

  /**
   * Initializes a new active WARC for an AU on the base path with the most free space.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return The {@link Path} to the new active WARC for this AU.
   * @throws IOException
   */
  public Path initAuActiveWarc(String collectionId, String auid) throws IOException {
    log.trace("collection = {}", collectionId);
    log.trace("auid = {}", auid);

    initAu(collectionId, auid);

    // Determine which AU path to use by comparing available space
    Path[] auPaths = getAuPaths(collectionId, auid);
    Path auPath = Arrays.stream(auPaths)
        .sorted((a, b) -> (int) (getFreeSpace(b) - getFreeSpace(a)))
        .findFirst()
        .orElse(null);

    if (auPath == null) {
      throw new IllegalStateException("Data store is not initialized correctly");
    }

    // FIXME
    Path warcFile = auPath.resolve("artifacts").resolve(generateActiveWarcName(collectionId, auid));

//    initWarc(warcFile);

    synchronized (auActiveWarcsMap) {
      CollectionAuidPair aukey = new CollectionAuidPair(collectionId, auid);
      List<Path> paths = auActiveWarcsMap.getOrDefault(aukey, new ArrayList<>());
      paths.add(warcFile);
      auActiveWarcsMap.put(aukey, paths);
    }

    return warcFile;
  }

  /**
   * "Seals" the active WARC of an AU in permanent storage from further writes.
   *
   * @param collectionId A {@link String} containing the collection ID of the AU.
   * @param auid         A {@link String} containing the AUID of the AU.
   */
  public void sealActiveWarc(String collectionId, String auid, Path warcPath) {
    log.trace("collection = {}", collectionId);
    log.trace("auid = {}", auid);
    log.trace("warcPath = {}", warcPath);

    synchronized (auActiveWarcsMap) {
      CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);

      if (auActiveWarcsMap.containsKey(key)) {
        List<Path> activeWarcs = auActiveWarcsMap.get(key);

        if (!activeWarcs.remove(warcPath)) {
          log.debug2("Attempted to seal an active WARC of an AU that is not active!");
        }

        auActiveWarcsMap.put(key, activeWarcs);
      } else {
        log.debug2("Attempted to seal an active WARC of an AU having no active WARCs!");
      }
    }
  }

  // *******************************************************************************************************************
  // * TEMPORARY WARCS LIFECYCLE
  // *******************************************************************************************************************

  protected void runGarbageCollector() {
    scheduledExecutor.submit(new GarbageCollectTempWarcsTask());
  }

  private class GarbageCollectTempWarcsTask implements Runnable {
    @Override
    public void run() {
      garbageCollectTempWarcs();
    }
  }

  /**
   * Removes temporary WARCs in this data store that are no longer needed by the data store.
   *
   * @throws IOException
   */
  protected void garbageCollectTempWarcs() {
    log.debug("Starting garbage collection of all temporary WARCs");

    Path[] tmpWarcBasePaths = getTmpWarcBasePaths();
    Arrays.stream(tmpWarcBasePaths).forEach(this::garbageCollectTempWarcs);
  }

  /**
   * Removes temporary WARCs under the given temporary WARCs directory that are no longer needed by the data store.
   *
   * @param tmpWarcBasePath
   */
  protected void garbageCollectTempWarcs(Path tmpWarcBasePath) {
    try {
      // Find all temporary WARCs under this temporary WARC base path
      Collection<Path> tmpWarcs = findWarcs(tmpWarcBasePath);

      // Debugging
      log.debug2("Found {} temporary WARCs [tmpWarcBasePath: {}]", tmpWarcs.size(), tmpWarcBasePath);
      log.trace("tmpWarcs = {}", tmpWarcs);

      for (Path tmpWarc : tmpWarcs) {
        WarcFile warcFile = null;

        log.trace("Processing [tmpWarc = {}]", tmpWarc);

        // Remove the temporary WARC from the pool if it is active there
        synchronized (tmpWarcPool) {

          if (tmpWarcPool.isInUse(tmpWarc)) {
            // Temporary WARC is in use - skip it
            log.debug2("Temporary WARC file is in use; will attempt a GC again later");
            continue;

          } else if (tmpWarcPool.isInPool(tmpWarc)) {
            // Temporary WARC is a member of the pool but not currently in use - process it
            warcFile = tmpWarcPool.borrowWarcFile(tmpWarc);

            if (warcFile == null) {
              // This message is worth paying attention to if logged - it may indicate a problem with synchronization
              log.debug2("Could not borrow temporary WARC file [tmpWarc: {}]", tmpWarc);
              continue;
            }

          } else {
            // Temporary WARC is not a member of the pool - process it anyway
          }

        }

        // Determine whether this temporary WARC should be removed
        try {
          if (isTempWarcRemovable(tmpWarc)) {
            // Yes: Remove the temporary WARC from storage
            log.debug2("Removing temporary WARC file [tmpWarc: {}]", tmpWarc);
            tmpWarcPool.removeWarcFile(warcFile);
            removeWarc(tmpWarc);
          } else {
            // NO - Return the temporary WARC to the pool if we borrowed it from the pool earlier
            if (warcFile != null) {
              synchronized (tmpWarcPool) {
                log.debug2("Returning {} to temporary WARC pool", warcFile.getPath());
                tmpWarcPool.returnWarcFile(warcFile);
              }
            }
          }
        } catch (IOException e) {
          log.error(
              "Caught IOException while trying to GC temporary WARC file [tmpWarcPath: {}]",
              tmpWarc,
              e
          );
        }
      }

    } catch (IOException e) {
      log.error(
          "Caught IOException while trying to find temporary WARC files [tmpWarcBasePath: {}]: {}",
          tmpWarcBasePath,
          e
      );
    }

    log.debug("Finished GC of temporary WARC files [tmpWarcBasePath: {}]", tmpWarcBasePath);
  }

  /**
   * Reloads the temporary WARCs under all the base paths configured in this data store.
   *
   * @throws IOException
   */
  protected void reloadTemporaryWarcs() throws IOException {
    if (artifactIndex == null) {
      throw new IllegalStateException("Cannot reload data store state from temporary WARCs without an artifact index");
    }

    for (Path tmpWarcBasePath : getTmpWarcBasePaths()) {
      reloadTemporaryWarcs(tmpWarcBasePath);
    }
  }

  /**
   * Reads and reloads state from temporary WARCs, including the requeuing of copy tasks of committed artifacts from
   * temporary to permanent storage. Removes temporary WARCs if eligible:
   * <p>
   * A temporary WARC may be removed if all the records contained within it are the serializations of artifacts that are
   * either uncommitted-but-expired, committed-and-moved-to-permanent-storage, or deleted.
   */
  protected void reloadTemporaryWarcs(Path tmpWarcBasePath) throws IOException {
    log.info("Reloading temporary WARCs from {}", tmpWarcBasePath);

    Collection<Path> tmpWarcs = findWarcs(tmpWarcBasePath);

    log.debug("Found {} temporary WARCs: {}", tmpWarcs.size(), tmpWarcs);

    // Iterate over the temporary WARC files that were found
    for (Path tmpWarc : tmpWarcs) {
      boolean isTmpWarcRemovable = true;

      log.debug2("tmpWarc = {}", tmpWarc);

      // Sanity check
      if (tmpWarcPool.isInPool(tmpWarc)) {
        log.debug2("Temporary file is already in pool! [tmpWarc: {}]", tmpWarc);
        continue;
      }

      // ********************************************************
      // Determine whether all records in this WARC are removable
      // ********************************************************

      try (InputStream warcStream = markAndGetInputStream(tmpWarc)) {
        // Get an ArchiveReader (an implementation of Iterable) over ArchiveRecord objects
        ArchiveReader archiveReader = new UncompressedWARCReader(tmpWarc.toString(), warcStream);

        // Iterate over the WARC records
        for (ArchiveRecord record : archiveReader) {
          boolean isRecordRemovable = false;

          // ArchiveRecordHeader#getLength() does not include the pair of CRLFs at the end of every WARC record so
          // we add four bytes to the length
          long recordLength = record.getHeader().getLength() + 4L;

          ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);
          String artifactId = artifactData.getIdentifier().getId();
          artifactData.setStorageUrl(makeWarcRecordStorageUrl(tmpWarc, record.getHeader().getOffset(), recordLength));

          log.trace("artifactData.getStorageUrl() = {}", artifactData.getStorageUrl());

          // Resume artifact lifecycle base on the artifact's state
          switch (
              getArtifactState(
                  isArtifactExpired(record),
                  isArtifactDeleted(artifactData.getIdentifier()),
                  artifactIndex.getArtifact(artifactId))
          ) {
            case NOT_INDEXED:
              // An artifact was found in a temporary WARC that is not indexed for some reason - index the artifact now
              log.debug("Did not find artifact in the index; indexing now [artifactId: {}]", artifactId);
              artifactIndex.indexArtifact(artifactData);
              break;
            case UNCOMMITTED:
              break;
            case COMMITTED:
              // Requeue the copy of this artifact from temporary to permanent storage
              log.trace("Requeuing move to permanent storage for artifact [artifactId: {}]", artifactId);

              try {
                stripedExecutor.submit(new CommitArtifactTask(artifactIndex.getArtifact(artifactId)));
              } catch (RejectedExecutionException e) {
                log.warn(
                    "Could not requeue copy of artifact to permanent storage [artifactId: {}]",
                    artifactId, e
                );
              }

              break;
            case EXPIRED:
            case COPIED:
            case DELETED:
              log.debug2("Temporary WARC record is removable [warcId: {}]", record.getHeader().getRecordIdentifier());
              isRecordRemovable = true;
          }

          // All records must be removable for temporary WARC file to be removable
          isTmpWarcRemovable &= isRecordRemovable;
        }

      } catch (URISyntaxException use) {
        log.error("Caught URISyntaxException while trying to reload temporary WARC: {}: {}", tmpWarc, use);
        continue;
      } catch (IOException e) {
        log.error("Caught IOException while trying to reload temporary WARC: {}: {}", tmpWarc, e);
        continue;
      }

      // ***************************************
      // Handle the result of isTmpWarcRemovable
      // ***************************************

      log.trace("isTmpWarcRemovable = {}", isTmpWarcRemovable);
      log.trace("isInUse({}) = {}", tmpWarc, TempWarcInUseTracker.INSTANCE.isInUse(tmpWarc));

      if (isTmpWarcRemovable && !TempWarcInUseTracker.INSTANCE.isInUse(tmpWarc)) {
        log.debug2("Removing temporary WARC [{}]", tmpWarc);

        try {
          removeWarc(tmpWarc);
        } catch (IOException e) {
          log.warn("Could not remove removable temporary WARC file [tmpWarc: {}]", tmpWarc);
        }
      } else {
        // Add this WARC file to temporary WARC pool
        long tmpWarcFileLen = getWarcLength(tmpWarc);
        tmpWarcPool.addWarcFile(new WarcFile(tmpWarc, tmpWarcFileLen));
      }
    }

    log.debug("Finished reloading temporary WARCs from {}", tmpWarcBasePath);
  }

  /**
   * Determines whether a temporary WARC file is removable.
   * <p>
   * A temporary WARC file is removable if all of the WARC records contained within it may be removed.
   * <p>
   * FIXME: This is in service of the temporary WARC garbage collector. This is slightly different from reloading
   * temporary WARCs, which may resume the artifact's lifecycle depending on its state.
   *
   * @param tmpWarc A {@link Path} containing the path to a temporary WARC file.
   * @return A {@code boolean} indicating whether the temporary WARC may be removed.
   * @throws IOException
   */
  private boolean isTempWarcRemovable(Path tmpWarc) throws IOException {
    try (InputStream warcStream = markAndGetInputStream(tmpWarc)) {
      // Get a WARCReader to the temporary WARC
      ArchiveReader archiveReader = WARCReaderFactory.get(tmpWarc.toString(), warcStream, true);

      for (ArchiveRecord record : archiveReader) {
        if (!isTempWarcRecordRemovable(record)) {
          // Temporary WARC contains a WARC record that is still needed
          return false;
        }
      }
    }

    // Reached here because none of the records in the temporary WARC file are needed
    // Check whether the temporary WARC file is in use elsewhere.
    return !TempWarcInUseTracker.INSTANCE.isInUse(tmpWarc);
  }

  /**
   * Determines whether a single WARC record is removable.
   * <p>
   * It is removable if it is expired and not committed, or expired and committed but not pending a copy to permanent
   * storage. If unexpired, it is removable if committed and not pending a copy to permanent storage.
   *
   * @param record An {@path ArchiveRecord} representing a WARC record in a temporary WARC file.
   * @return A {@code boolean} indicating whether this WARC record is removable.
   */
  private boolean isTempWarcRecordRemovable(ArchiveRecord record) {
    // Get WARC record headers
    ArchiveRecordHeader headers = record.getHeader();

    // Get artifact ID from WARC header
    String artifactId = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_ID_KEY);

    // Get the WARC record ID and type
    String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

    // WARC records not of type "response" or "resource" are okay to remove
    if (!recordType.equalsIgnoreCase(WARCRecordType.response.toString()) &&
        !recordType.equalsIgnoreCase(WARCRecordType.resource.toString())) {
      return true;
    }

    try {
      ArtifactState artifactState = getArtifactState(
          isArtifactExpired(record),
          isArtifactDeleted(ArtifactDataFactory.buildArtifactIdentifier(headers)),
          artifactIndex.getArtifact(artifactId)
      );

      log.trace("artifactId: {}, artifactState = {}", artifactId, artifactState);

      // Handle the different life cycle states in which the artifact may be.
      switch (artifactState) {
        case NOT_INDEXED:
        case COMMITTED:
        case UNCOMMITTED:
          return false;
        case EXPIRED:
        case COPIED:
        case DELETED:
          return true;
      }
    } catch (URISyntaxException use) {
      log.error("Could not get artifact state: {}", use);
    } catch (IOException e) {
      log.error("Could not retrieve artifact from index: {}", e);
    }

    return false;
  }

  // *******************************************************************************************************************
  // * ARTIFACT LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Provides the state of an Artifact in its life cycle.
   *
   * @param expired  A boolean with the indication of whether the artifact record has
   *                 expired.
   * @param artifact An Artifact with the Artifact.
   * @return an ArtifactState with the state of the Artifact.
   */
  protected ArtifactState getArtifactState(boolean expired, boolean deleted, Artifact artifact) throws URISyntaxException {
    // Check whether the artifact is deleted
    if (deleted) {
      return ArtifactState.DELETED;
    }

    // Check whether the Artifact is in the index.
    if (artifact != null) {
      // Yes: Check whether its storage URL is not in a temporary WARC file.
      if (!isTmpStorage(getPathFromStorageUrl(new URI(artifact.getStorageUrl())))) {

        // Yes: It has been copied to permanent storage already.
        log.debug2("ArtifactState = {}", ArtifactState.COPIED);
        return ArtifactState.COPIED;
      }

      // No: Check whether the Artifact is committed.
      if (artifact.getCommitted()) {
        // Yes.
        log.debug2("ArtifactState = {}", ArtifactState.COMMITTED);
        return ArtifactState.COMMITTED;
      }
    }

    // The Artifact is not committed or copied: Check whether the Artifact has expired.
    if (expired) {
      // Yes.
      log.debug2("ArtifactState = {}", ArtifactState.EXPIRED);
      return ArtifactState.EXPIRED;
    }

    // No, the Artifact has not expired: Check whether the Artifact is not in the index.
    if (artifact == null) {
      // Yes.
      log.debug2("ArtifactState = {}", ArtifactState.NOT_INDEXED);
      return ArtifactState.NOT_INDEXED;
    }

    // No: The Artifact is indexed.
    log.debug2("ArtifactState = {}", ArtifactState.UNCOMMITTED);
    return ArtifactState.UNCOMMITTED;
  }

  /**
   * Returns a boolean indicating whether the an artifact is expired by reading the headers of its WARC record.
   *
   * @param record The {@link ArchiveRecord} instance of the artifact.
   * @return A {@code boolean} indicating whether the artifact is expired.
   */
  private boolean isArtifactExpired(ArchiveRecord record) {
    // Get WARC record headers
    ArchiveRecordHeader headers = record.getHeader();

    // Parse WARC-Date field and determine if this record / artifact is expired (same date should be in index)
    String warcDateHeader = headers.getDate();
    Instant created = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(warcDateHeader));
    Instant expires = created.plus(getUncommittedArtifactExpiration(), ChronoUnit.MILLIS);
    boolean expired = expires.isBefore(Instant.now());

    return expired;
  }

  /**
   * Returns a boolean indicating whether an artifact is marked as deleted in the journal.
   *
   * @param aid The {@link ArtifactIdentifier} of the artifact to check.
   * @return A {@code boolean} indicating whether the artifact is marked as deleted.
   * @throws IOException
   */
  protected boolean isArtifactDeleted(ArtifactIdentifier aid) throws IOException {
    RepositoryArtifactMetadata metadata = getRepositoryMetadata(aid);

    if (metadata != null) {
      // YES: Repository metadata journal entry found for this artifact
      return metadata.isDeleted();
    }

    return true;
  }

  /**
   * Returns a boolean indicating whether an artifact is marked as committed in the journal.
   *
   * @param aid The {@link ArtifactIdentifier} of the artifact to check.
   * @return A {@code boolean} indicating whether the artifact is marked as committed.
   * @throws IOException
   */
  protected boolean isArtifactCommitted(ArtifactIdentifier aid) throws IOException {
    RepositoryArtifactMetadata metadata = getRepositoryMetadata(aid);

    if (metadata != null) {
      // YES: Repository metadata journal entry found for this artifact
      return metadata.isCommitted();
    }

    return false;
  }

  // *******************************************************************************************************************
  // * GETTERS AND SETTERS
  // *******************************************************************************************************************

  /**
   * Returns the number of milliseconds after the creation date of an artifact, that an uncommitted artifact will be
   * marked expired.
   *
   * @return A {@code long}
   */
  public long getUncommittedArtifactExpiration() {
    return uncommittedArtifactExpiration;
  }

  /**
   * Sets the number of milliseconds after the creation date of an artifact, that an uncommitted artifact will be marked
   * expired.
   *
   * @param milliseconds A {@code long}
   */
  public void setUncommittedArtifactExpiration(long milliseconds) {
    this.uncommittedArtifactExpiration = milliseconds;
  }

  /**
   * Returns the number of bytes
   *
   * @return
   */
  public long getThresholdWarcSize() {
    return thresholdWarcSize;
  }

  /**
   * <p>
   * Sets the threshold size above which a new WARC file should be started.
   * Legal values are a positive number of bytes and zero for unlimited;
   * negative values are illegal.
   * </p>
   *
   * @param threshold
   * @throws IllegalArgumentException if the given value is negative.
   */
  public void setThresholdWarcSize(long threshold) {
    if (threshold < 0L) {
      throw new IllegalArgumentException("Threshold size must be positive (or zero for unlimited)");
    }

    thresholdWarcSize = threshold;
  }

  /**
   * Configures the artifact index associated with this WARC artifact data store.
   * <p>
   * Should only be used in testing.
   *
   * @param artifactIndex The {@code ArtifactIndex} instance to associate with this WARC artifact data store.
   */
  protected void setArtifactIndex(ArtifactIndex artifactIndex) {
//    if (artifactIndex == null) {
//      throw new IllegalArgumentException("Null artifact index");
//    }

//    if (this.artifactIndex != null && this.artifactIndex != artifactIndex) {
//      throw new IllegalStateException("Artifact index already set");
//    }

    this.artifactIndex = artifactIndex;
  }

  /**
   * Return the artifact index associated with this WARC artifact data store.
   *
   * @return The {@code ArtifactIndex} associated with this WARC artifact data store.
   */
  public ArtifactIndex getArtifactIndex() {
    return artifactIndex;
  }

  // *******************************************************************************************************************
  // * ArtifactDataStore INTERFACE IMPLEMENTATION
  // *******************************************************************************************************************

  /**
   * Stores artifact data to this WARC artifact data store by appending to an available temporary WARC from a pool of
   * temporary WARCs. This strategy was chosen to allow multiple threads to add to this artifact data store
   * simultaneously.
   *
   * @param artifactData An instance of {@code ArtifactData} to store to this artifact data store.
   * @return
   * @throws IOException
   */
  @Override
  public Artifact addArtifactData(ArtifactData artifactData) throws IOException {
    if (artifactData == null) {
      throw new IllegalArgumentException("Null artifact data");
    }

    // Get the artifact identifier
    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    if (artifactId == null) {
      throw new IllegalArgumentException("Artifact data has null identifier");
    }

    log.debug("Adding artifact [artifactId: {}]", artifactId);

    try {
      // ********************************
      // Write artifact to temporary WARC
      // ********************************

      // Serialize artifact into WARC record and write it to a DFOS
      // FIXME: This is done to get the length for the serialization (WARC record)
      DeferredTempFileOutputStream dfos = new DeferredTempFileOutputStream((int) DEFAULT_DFOS_THRESHOLD, "addArtifactData");
      long recordLength = writeArtifactData(artifactData, dfos);
      dfos.close();

      // Determine which base path to use based on which has the most available space
      Path basePath = Arrays.stream(basePaths)
          .max((a, b) -> (int) (getFreeSpace(a) - getFreeSpace(b)))
          .filter(bp -> getFreeSpace(bp) >= recordLength)
          .get();

      if (basePath == null) {
        throw new IOException("Could not add artifact: No base path available");
      }

      // Get a temporary WARC from the temporary WARC pool for this base path
      WarcFile tmpWarcFile = tmpWarcPool.findWarcFile(basePath, recordLength);
      Path tmpWarcFilePath = tmpWarcFile.getPath();

      log.trace("tmpWarcFile = {}", tmpWarcFile);

      // Initialize the WARC
      initWarc(tmpWarcFilePath);

      // The offset for the record to be appended to this WARC is the length of the WARC file (i.e., its end)
      long offset = getWarcLength(tmpWarcFilePath);

      // Keep track of the number of bytes written to this WARC
      long bytesWritten = 0;

      // Write serialized artifact to temporary WARC file
      try (OutputStream output = getAppendableOutputStream(tmpWarcFilePath)) {

        // Get an InputStream containing the serialized artifact from the DFOS
        try (InputStream input = dfos.getInputStream()) {

          // Write the serialized artifact to the temporary WARC file
          bytesWritten = IOUtils.copy(input, output);

          // Debugging
          log.debug2("Wrote {} bytes starting at byte offset {} to {}; size is now {}",
              recordLength,
              offset,
              tmpWarcFilePath,
              offset + recordLength
          );

          // Sanity check on bytes written
          if (bytesWritten != recordLength) {
            log.error(
                "Wrote unexpected number of bytes [bytesWritten: {}, recordLength: {}, artifactId: {}, tmpWarcPath: {}]",
                bytesWritten,
                recordLength,
                artifactId.getId(),
                tmpWarcFilePath
            );

            // TODO: Rollback? Subsequent appends are pointed to by storage URL, which has an offset, so we don't care
            //       but serial parsers of WARC files might be confused by an incomplete WARC record.

            throw new IOException("Wrote unexpected number of bytes");
          }

        }
      } finally {
        // Always update the temporary WARC's stats and return it to the pool
        tmpWarcFile.setLength(offset + bytesWritten);
        tmpWarcPool.returnWarcFile(tmpWarcFile);
      }

      // **********************************
      // Write artifact metadata to journal
      // **********************************

      artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(artifactId, false, false));
      updateArtifactMetadata(basePath, artifactId, artifactData.getRepositoryMetadata());

      // ******************
      // Index the artifact
      // ******************

      // Set artifact data storage URL and repository state
      artifactData.setStorageUrl(makeWarcRecordStorageUrl(tmpWarcFilePath, offset, recordLength));
      artifactIndex.indexArtifact(artifactData);
//      Artifact artifact = artifactIndex.indexArtifact(artifactData);

      // Create a new Artifact object to return
      Artifact artifact = new Artifact(
          artifactId,
          false,
          artifactData.getStorageUrl().toString(),
          artifactData.getContentLength(),
          artifactData.getContentDigest()
      );

      // Set the artifact collection date
      artifact.setCollectionDate(artifactData.getCollectionDate());

      log.debug("Added artifact {}", artifact);

      return artifact;

    } catch (Exception e) {

      log.fatal("Could not add artifact data", e);
      throw new IOException("Could not add artifact data", e);

    }
  }

  /**
   * Retrieves the {@link ArtifactData} of an artifact
   *
   * @param artifact An {@link Artifact} instance containing a reference to the artifact data to retrieve from storage.
   * @return The {@link ArtifactData} of the artifact.
   * @throws IOException
   */
  @Override
  public ArtifactData getArtifactData(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Artifact is null");
    }

    try {
      String artifactId = artifact.getId();
      URI storageUrl = new URI(artifact.getStorageUrl());
      Path warcFilePath = getPathFromStorageUrl(storageUrl);

      log.debug2("Retrieving artifact data [artifactId: {}, storageUrl: {}]", artifactId, storageUrl);

      // Guard against deleted or non-existent artifact
      if (!artifactIndex.artifactExists(artifactId) || isArtifactDeleted(artifact.getIdentifier())) {
        return null;
      }

      // Open an InputStream from the WARC file and get the WARC record representing this artifact data
      InputStream warcStream = getInputStreamFromStorageUrl(storageUrl);

      if (isTmpStorage(warcFilePath)) {
        // Increment the counter of times that the file is in use.
        TempWarcInUseTracker.INSTANCE.markUseStart(warcFilePath);

        // Wrap the stream with a CloseCallbackInputStream with a callback that will mark the end of the use of this file
        // when close() is called.
        warcStream = new CloseCallbackInputStream(
            warcStream,
            closingWarcFilePath -> {
              // Decrement the counter of times that the file is in use.
              TempWarcInUseTracker.INSTANCE.markUseEnd((Path) closingWarcFilePath);
            },
            warcFilePath
        );
      }

      // Create WARCRecord object from InputStream
      WARCRecord warcRecord = new WARCRecord(warcStream, getClass().getSimpleName(), 0L);

      // Convert the WARCRecord object to an ArtifactData
      ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(warcRecord); // FIXME: Move to ArtifactDataUtil or ArtifactData

      // Save the underlying input stream so that it can be closed when needed.
      artifactData.setClosableInputStream(warcStream);

      // Set ArtifactData properties
      artifactData.setIdentifier(artifact.getIdentifier());
      artifactData.setStorageUrl(URI.create(artifact.getStorageUrl()));
      artifactData.setContentLength(artifact.getContentLength());
      artifactData.setContentDigest(artifact.getContentDigest());

      // Set artifact's repository metadata state
      artifactData.setRepositoryMetadata(getRepositoryMetadata(artifact.getIdentifier()));

      // Return an ArtifactData from the WARC record
      return artifactData;

    } catch (URISyntaxException e) {
      // This should never happen since storage URLs are internal
      log.error(String.format("Could not get artifact data [storageUrl: %s]", artifact.getStorageUrl()), e);
      throw new IllegalArgumentException("Bad storage URL");

    } catch (Exception e) {
      log.error(String.format("Could not get artifact data [storageUrl: %s]", artifact.getStorageUrl()), e);
      log.trace("artifact = {}", artifact);
      log.trace("storageUrl = {}", artifact.getStorageUrl());
      throw e;
    }
  }

  /**
   * Updates the repository metadata of artifact by appending an entry to the repository metadata journal in the
   * artifact's AU.
   *
   * @param basePath         A {@link Path} containing the base path of the artifact.
   * @param artifactId       The {@link ArtifactIdentifier} of the artifact to update.
   * @param artifactMetadata The new {@link RepositoryArtifactMetadata}.
   * @return The {@link RepositoryArtifactMetadata} that was recorded.
   * @throws IOException
   */
  // TODO: Generalize this to arbitrary metadata
  public synchronized RepositoryArtifactMetadata updateArtifactMetadata(
      Path basePath,
      ArtifactIdentifier artifactId,
      RepositoryArtifactMetadata artifactMetadata
  ) throws IOException {

    Objects.requireNonNull(artifactId, "Artifact identifier is null");
    Objects.requireNonNull(artifactMetadata, "Repository artifact metadata is null");

    Path auMetadataWarcPath = getAuMetadataWarcPath(basePath, artifactId, RepositoryArtifactMetadata.LOCKSS_METADATA_ID);

    log.trace("artifactId = {}", artifactId);
    log.trace("auMetadataWarcPath = {}", auMetadataWarcPath);
    log.trace("artifactMetadata = {}", artifactMetadata.toJson());

    // Initialize metadata WARC file
    initWarc(auMetadataWarcPath);

    try (OutputStream output = getAppendableOutputStream(auMetadataWarcPath)) {
      // Append WARC metadata record to the journal
      WARCRecordInfo metadataRecord = createWarcMetadataRecord(artifactId.getId(), artifactMetadata);
      writeWarcRecord(metadataRecord, output);
      output.flush();
    }

    log.debug2("Finished updateArtifactMetadata() for [artifactId: {}]", artifactId.getId());

    return artifactMetadata;
  }

  /**
   * Commits an artifact from temporary to permanent storage.
   *
   * @param artifact The {@link Artifact} to commit to permanent storage.
   * @return An {@link Future<Artifact>} reflecting the new committed state and storage URL.
   * @throws IOException
   */
  @Override
  public Future<Artifact> commitArtifactData(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Artifact is null");
    }

    log.debug2("Committing artifact {} in AU {}", artifact.getId(), artifact.getAuid());

    try {
      // Read current state of this artifact from the repository metadata journal
      RepositoryArtifactMetadata artifactState = getRepositoryMetadata(artifact.getIdentifier());

      // Determine what action to take based on the state of the artifact
      // FIXME: Potential for race condition? What if the state of the artifact changes?
      switch (getArtifactState(false, artifactState.isDeleted(), artifact)) {
        case UNCOMMITTED:
          // Record new committed state to artifact data repository metadata journal
          artifact.setCommitted(true);
          artifactState.setCommitted(true);

          Path basePath = getBasePathFromStorageUrl(new URI(artifact.getStorageUrl()));

          log.trace("storageUrl = {}", artifact.getStorageUrl());
          log.trace("basePath = {}", basePath);

          updateArtifactMetadata(basePath, artifact.getIdentifier(), artifactState);

        case COMMITTED:
          // Submit the task to copy the artifact data from temporary to permanent storage
          return stripedExecutor.submit(new CommitArtifactTask(artifact));

        case COPIED:
          // This artifact is already marked committed and is in permanent storage. Wrap in Future and return it.
          return new CompletedFuture<>(artifact);

        case DELETED:
          log.warn("Cannot commit deleted artifact [artifactId: {}]", artifact.getId());

        default: // Includes UNKNOWN, NOT_INDEXED, EXPIRED
          return null;
      }
    } catch (URISyntaxException e) {
      // This should never happen since storage URLs are internal
      throw new IllegalStateException(e);
    }
  }

  /**
   * Implementation of {@link Callable} that commits an artifact from temporary to permanent storage.
   * <p>
   * This is implemented as a {@link StripedCallable} because we maintain one active WARC file per AU in which to commit
   * artifacts permanently.
   */
  private class CommitArtifactTask implements StripedCallable<Artifact> {
    private Artifact artifact;

    /**
     * Constructor of {@link CommitArtifactTask}.
     *
     * @param artifact The {@link Artifact} whose artifact data should be copied from temporary to permanent storage.
     */
    public CommitArtifactTask(Artifact artifact) {
      if (artifact == null) {
        throw new IllegalArgumentException("Artifact is null");
      }

      this.artifact = artifact;
    }

    /**
     * Returns this equivalence class or "stripe" that this task belongs to.
     *
     * @return
     */
    @Override
    public Object getStripe() {
      return new CollectionAuidPair(artifact.getCollection(), artifact.getAuid());
    }

    /**
     * Moves the WARC record of an artifact from temporary storage to a WARC in permanent storage, and updates the
     * storage URL if successful.
     */
    @Override
    public Artifact call() throws Exception {
      // Get the temporary WARC record location
      WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(new URI(artifact.getStorageUrl()));
      long recordOffset = loc.getOffset();
      long recordLength = loc.getLength();

      // Get an active WARC (i.e., in permanent storage) of this AU to append the artifact to
      Path dst = getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid());

      // Initialize the active WARC if necessary
      initWarc(dst);

      // Artifact will be appended as a WARC record to this WARC file so its offset is the current length of the file
      long warcLength = getWarcLength(dst);

      // **************************************************************************
      // Determine whether we should seal this active WARC instead of writing to it
      // **************************************************************************

      boolean sealBeforeAppend = false;

      // Calculate waste space in last block in two strategies
      long wasteSealing = (getBlockSize() - (warcLength % getBlockSize())) % getBlockSize();
      long wasteAppending = (getBlockSize() - ((warcLength + recordLength) % getBlockSize())) % getBlockSize();

      log.debug2(
          "[warcLength: {}, recordOffset: {}, recordLength: {}, wasteSealing: {}, wasteAppending: {}]",
          warcLength, recordOffset, recordLength, wasteSealing, wasteAppending
      );

      if (warcLength < getThresholdWarcSize()) {
        if (warcLength + recordLength <= getThresholdWarcSize()) {
          // Append
//        sealBeforeAppend = false;
        } else {
          // Appending this record would trip the file over the size threshold
          if (wasteSealing > wasteAppending) {
            // Append
//          sealBeforeAppend = false;
          } else {
            // Seal then append
            sealBeforeAppend = true;
          }
        }
      } else {
        // The WARC file already exceeds the WARC file size threshold
        if (recordLength <= wasteSealing) {
          // Record fits within the space in the last, already allocated block: Fill it (append)
//        sealBeforeAppend = false;
        } else {
          // Seal then append: Do not allocate new block(s) since we have already exceeded the size threshold
          sealBeforeAppend = true;
        }
      }

      // **********************
      // Seal WARC if necessary
      // **********************

      if (sealBeforeAppend) {
        // Seal active WARC
        sealActiveWarc(artifact.getCollection(), artifact.getAuid(), dst);

        // Get path to new active WARC and ensure it exists
        dst = getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid());
        initWarc(dst);
        warcLength = getWarcLength(dst);
      }

      // *********************************
      // Append WARC record to active WARC
      // *********************************

      try (OutputStream output = getAppendableOutputStream(dst)) {
        try (InputStream is = markAndGetInputStreamAndSeek(loc.getPath(), loc.getOffset())) {
          long bytesWritten = StreamUtils.copyRange(is, output, 0, recordLength - 1);

          log.debug2("Moved artifact {}: Wrote {} of {} bytes starting at byte offset {} to {}; size of WARC file is now {}",
              artifact.getIdentifier().getId(),
              bytesWritten,
              recordLength,
              warcLength,
              dst,
              warcLength + recordLength
          );
        }
      }

      // Immediately seal if we've gone over the size threshold and filled all the blocks perfectly
      if (!sealBeforeAppend) {
        if ((warcLength + recordLength >= getThresholdWarcSize()) && ((warcLength + recordLength) % getBlockSize() == 0)) {
          sealActiveWarc(artifact.getCollection(), artifact.getAuid(), dst);
        }
      }

      // ******************
      // Update storage URL
      // ******************

      // Set the artifact's new storage URL and update the index
      artifact.setStorageUrl(makeWarcRecordStorageUrl(dst, warcLength, recordLength).toString());
      artifactIndex.updateStorageUrl(artifact.getId(), artifact.getStorageUrl());

      // *********************
      // Update artifact state
      // *********************

      Path basePath = getBasePathFromStorageUrl(new URI(artifact.getStorageUrl()));

      RepositoryArtifactMetadata state = new RepositoryArtifactMetadata(
          artifact.getIdentifier(),
          artifact.getCommitted(),
          false
      );

      updateArtifactMetadata(basePath, artifact.getIdentifier(), state);

      return artifact;
    }
  }

  /**
   * Removes an artifact from this data store.
   *
   * @param artifact The {@link Artifact} whose {@link ArtifactData} will be removed from this artifact store.
   * @throws IOException
   */
  @Override
  public void deleteArtifactData(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    // Guard against non-existent artifact
    if (!artifactIndex.artifactExists(artifact.getId())) {
      log.warn("Artifact doesn't exist [artifactId: {}]", artifact.getId());
      return;
    }

    try {
      // Retrieve artifact data from current WARC file
      RepositoryArtifactMetadata repoMetadata = getRepositoryMetadata(artifact.getIdentifier());

      if (!repoMetadata.isDeleted()) {
        // Set deleted flag
        repoMetadata.setDeleted(true);

        // Write new state to artifact data repository metadata journal
        updateArtifactMetadata(
            getBasePathFromStorageUrl(new URI(artifact.getStorageUrl())),
            artifact.getIdentifier(),
            repoMetadata
        );

        // Update artifact index
        artifactIndex.deleteArtifact(artifact.getId());

        // TODO: Remove artifact from storage - cutting and splicing WARC files is expensive so we just leave the
        //       artifact in place for now.
      } else {
        log.warn("Artifact is already deleted [artifact: {}]", artifact);
      }
    } catch (URISyntaxException e) {
      // This should never happen since storage URLs are internal and always valid
      log.error(
          "URISyntaxException caught; could not delete artifact [artifactId: {}, storageUrl: {}]",
          artifact.getId(),
          artifact.getStorageUrl()
      );

      throw new IOException("Bad storage URL");
    } catch (Exception e) {
      log.error("Caught exception deleting artifact [artifact: {}]", artifact, e);
      throw e;
    }

    log.debug2("Deleted artifact [artifactId: {}]", artifact.getId());
  }

  /**
   * Returns an {@link InputStream} of the WARC record pointed to by a storage URL.
   *
   * @param storageUrl A {@link URI} containing the storage URL of the WARC record.
   * @return An {@link InputStream} of the WARC record pointed to by a storage URL.
   * @throws IOException
   */
  protected InputStream getInputStreamFromStorageUrl(URI storageUrl) throws IOException {
    WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(storageUrl);
    return getInputStreamAndSeek(loc.getPath(), loc.getOffset());
  }

  // *******************************************************************************************************************
  // * INNER CLASSES
  // *******************************************************************************************************************

  // TODO - Pull this out and along WarcFile?
  private static class WarcRecordLocation {
    private Path path;
    private long offset;
    private long length;

    public WarcRecordLocation(Path path, long offset, long length) {
      this.path = path;
      this.offset = offset;
      this.length = length;
    }

    public Path getPath() {
      return this.path;
    }

    public long getOffset() {
      return this.offset;
    }

    public long getLength() {
      return this.length;
    }

    public static WarcRecordLocation fromStorageUrl(URI storageUri) {
      // Get path to WARC file
      Path path = Paths.get(storageUri.getPath());

      // Get WARC record offset and length
      MultiValueMap queryArgs = parseQueryArgs(storageUri.getQuery());
      long offset = Long.parseLong((String) queryArgs.getFirst("offset"));
      long length = Long.parseLong((String) queryArgs.getFirst("length"));

      return new WarcRecordLocation(path, offset, length);
    }

    private static MultiValueMap<String, String> parseQueryArgs(String query) {
      MultiValueMap<String, String> queries = new LinkedMultiValueMap<>();

      if (query == null) {
        return queries;
      }

      String[] kvps = query.split("&");

      if (kvps.length > 0) {
        for (String kvp : query.split("&")) {
          String[] kv = kvp.split("=");
          queries.add(kv[0], kv[1]);
        }
      }

      return queries;
    }
  }

  // *******************************************************************************************************************
  // * INDEX REBUILD FROM DATA STORE
  // *******************************************************************************************************************

  /**
   * Rebuilds the provided index from WARCs within this WARC artifact data store.
   *
   * @param index The {@code ArtifactIndex} to rebuild and populate from WARCs within this WARC artifact data store.
   * @throws IOException
   */
  public void rebuildIndex(ArtifactIndex index) throws IOException {
    for (Path basePath : getBasePaths()) {
      rebuildIndex(index, basePath);
    }
  }

  /**
   * Rebuilds an artifact index from WARCs within this WARC artifact data store.
   *
   * @param basePath A {@code String} containing the base path of this WARC artifact data store.
   * @throws IOException
   */
  public void rebuildIndex(ArtifactIndex index, Path basePath) throws IOException {
//    if (isInitialized()) {
//      throw new IllegalStateException("Index rebuild only allowed while the data store is offline");
//    }

    Collection<Path> warcPaths = findWarcs(basePath);

    // Find WARCs in permanent storage
    Collection<Path> permanentWarcs = warcPaths
        .stream()
        .filter(path -> !isTmpStorage(path))
        .filter(path -> !path.endsWith("lockss-repo." + WARC_FILE_EXTENSION)) // Exclude repository metadata journals
        .collect(Collectors.toList());

    // Find WARCS in temporary storage
    Collection<Path> temporaryWarcs = warcPaths
        .stream()
        .filter(this::isTmpStorage)
        .collect(Collectors.toList());

    log.debug2("permanentWarcs = {}", permanentWarcs);
    log.debug2("temporaryWarcs = {}", temporaryWarcs);

    //// Reindex artifacts from permanent storage
    reindexArtifactsFromWarcs(index, permanentWarcs);

    //// Reindex artifacts from temporary storage
    reindexArtifactsFromWarcs(index, temporaryWarcs);

    // TODO: What follows is loading of artifact repository-specific metadata. It should be generalized to others.

    // Get a collection of paths to WARCs containing repository metadata
    Collection<Path> repoMetadataWarcFiles = warcPaths
        .stream()
        .filter(file -> file.endsWith("lockss-repo." + WARC_FILE_EXTENSION))
        .collect(Collectors.toList());

    // Load repository artifact metadata by "replaying" them
    for (Path metadataFile : repoMetadataWarcFiles) {
      replayRepositoryMetadata(index, metadataFile);
    }

  }

  public static final String LOCKSS_METADATA_ARTIFACTID_KEY = "artifactId";

  /**
   * Rebuilds an artifact index from a collection of WARCs.
   *
   * @param index             The {@link ArtifactIndex} to rebuild with the artifacts from the provided WARC files.
   * @param artifactWarcFiles A {@link Collection<Path>} containing the paths to the WARC files.
   * @throws IOException
   */
  private void reindexArtifactsFromWarcs(ArtifactIndex index, Collection<Path> artifactWarcFiles) throws IOException {
    // Reindex artifacts from temporary and permanent storage
    for (Path warcFile : artifactWarcFiles) {

      try (InputStream warcStream = markAndGetInputStream(warcFile)) {
        for (ArchiveRecord record : new UncompressedWARCReader("WarcArtifactDataStore", warcStream)) {
          log.trace(
              "Re-indexing artifact from WARC {} record {} from {}",
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
              warcFile
          );

          try {
            ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

            if (artifactData != null) {
              // Skip if already indexed
              if (index.artifactExists(artifactData.getIdentifier().getId())) {
                log.warn("Artifact {} is already indexed", artifactData.getIdentifier().getId());
                continue;
              }

              // ArchiveRecordHeader#getLength() does not include the pair of CRLFs at the end of every WARC record so
              // we add four bytes to the length
              long recordLength = record.getHeader().getLength() + 4L;

              // Set ArtifactData storage URL
              artifactData.setStorageUrl(makeWarcRecordStorageUrl(warcFile, record.getHeader().getOffset(), recordLength));

              // Default repository metadata for all ArtifactData objects to be indexed
              artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(
                  artifactData.getIdentifier(),
                  false,
                  false
              ));

              log.trace("artifactData({}).getStorageUrl() = {}",
                  artifactData.getIdentifier().getId(),
                  artifactData.getStorageUrl()
              );

              // Add artifact to the index
              index.indexArtifact(artifactData);
            }
          } catch (IOException e) {
            log.error("IOException caught while attempting to re-index WARC record {} from {}",
                record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                warcFile
            );

            throw e;
          }
        }
      } catch (IOException e) {
        log.error("IOException caught while attempting to re-index WARC file {}: {}", warcFile, e.getStackTrace());
        throw e;
      }
    }
  }

  // *******************************************************************************************************************
  // * JOURNAL OPERATIONS
  // *******************************************************************************************************************

  /**
   * Truncates a journal by rewriting it with only its most recent entry per artifact ID.
   *
   * @param journalPath A {@link Path} containing the path to the data store journal to truncate.
   * @throws IOException
   */
  protected void truncateMetadataJournal(Path journalPath) throws IOException {
    for (JSONObject json : readMetadataJournal(journalPath).values()) {

      // TODO: Move existing journal out of the way instead of removing it
      removeWarc(journalPath);

      // Parse the JSON into a RepositoryArtifactMetadata object
      RepositoryArtifactMetadata metadata = new RepositoryArtifactMetadata(json);

      // Initialize metadata WARC file
      initWarc(journalPath);

      try (OutputStream output = getAppendableOutputStream(journalPath)) {
        // Append WARC metadata record to the new journal
        WARCRecordInfo metadataRecord = createWarcMetadataRecord(metadata.getArtifactId(), metadata);
        writeWarcRecord(metadataRecord, output);
        output.flush();
      }

    }
  }

  /**
   * Reads an artifact's current repository metadata state from storage.
   *
   * @param aid An {@link ArtifactIdentifier}
   * @return The {@link RepositoryArtifactMetadata} of the artifact.
   * @throws IOException
   */
  protected RepositoryArtifactMetadata getRepositoryMetadata(ArtifactIdentifier aid) throws IOException {
    // FIXME: This should be okay but it would be more robust if it compared timestamps instead of returning first found
    //        in order of base paths processed.

    for (Path basePath : getBasePaths()) {
      Path journalPath = getAuMetadataWarcPath(basePath, aid, RepositoryArtifactMetadata.LOCKSS_METADATA_ID);

      Map<String, JSONObject> journal = readMetadataJournal(journalPath);

      JSONObject metadata = journal.get(aid.getId());

      if (metadata != null) {
        log.trace("metadata = {}", metadata.toString());
        return new RepositoryArtifactMetadata(metadata);
      }
    }

    log.warn("Could not find any journal entries for artifact [artifactId: {}]", aid.getId());
    return null;
  }

  /**
   * Reads the journal for a class of artifact metadata from a WARC file at a given path, and builds a map from artifact
   * ID to its most recently journaled metadata.
   *
   * @param journalPath A {@code String} containing the path to the WARC file containing artifact metadata.
   * @return A {@code Map<String, JSONObject>} mapping artifact ID to its latest metadata.
   * @throws IOException
   */
  /* TODO: This is slow and inefficient as the number of stale entries in the journal grows. Some external process
           should periodically prune the journal. Additionally, some sort of caching could be beneficial since this is
           called often.
   */
  protected Map<String, JSONObject> readMetadataJournal(Path journalPath) throws IOException {
    Map<String, JSONObject> metadata = new HashMap<>();

    log.trace("journalPath = {}", journalPath);

    try (InputStream warcStream = markAndGetInputStream(journalPath)) {

      for (ArchiveRecord record : new UncompressedWARCReader(getClass().getSimpleName(), warcStream)) {
        // Determine whether this is a WARC metadata type record
        boolean isWarcMetadataRecord = ((String) record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE))
            .equalsIgnoreCase(WARCRecordType.metadata.toString());

        // TODO Retrieve and sort journal entries by WARC date; return latest entry
//        log.trace("record.getDate() = {}", record.getHeader().getDate());

        if (isWarcMetadataRecord) {
          JSONObject json = new JSONObject(IOUtils.toString(record, DEFAULT_ENCODING));
          String artifactId = json.getString(LOCKSS_METADATA_ARTIFACTID_KEY);
          metadata.put(artifactId, json);
        }
      }

      return metadata;
    }
  }

  /**
   * Reads and replays repository metadata to a given artifact index.
   *
   * @param index        An {@code ArtifactIndex} to replay repository metadata to.
   * @param metadataFile A {@code String} containing the path to a repository metadata journal WARC file.
   * @throws IOException
   */
  private void replayRepositoryMetadata(ArtifactIndex index, Path metadataFile) throws IOException {
    for (JSONObject json : readMetadataJournal(metadataFile).values()) {
      // Parse the JSON into a RepositoryArtifactMetadata object
      RepositoryArtifactMetadata repoState = new RepositoryArtifactMetadata(json);

      // Get the artifact ID of this repository metadata
      String artifactId = repoState.getArtifactId();

      log.debug2("Replaying repository metadata for artifact {} from repository metadata file {}",
          artifactId,
          metadataFile
      );

      // Replay to artifact index
      if (index.artifactExists(artifactId)) {
        if (repoState.isDeleted()) {
          log.debug2("Removing artifact {} from index", artifactId);
          index.deleteArtifact(artifactId);
          continue;
        }

        if (repoState.isCommitted()) {
          log.debug2("Marking artifact {} as committed in index", artifactId);
          index.commitArtifact(artifactId);
        }
      } else {
        if (!repoState.isDeleted()) {
          log.error("Artifact {} referenced in journal and not deleted but artifact doesn't exist!", artifactId);
        }
      }
    }
  }

  // *******************************************************************************************************************
  // * WARC
  // *******************************************************************************************************************

  /**
   * Writes an artifact as a WARC response record to a given OutputStream.
   *
   * @param artifactData {@code ArtifactData} to write to the {@code OutputStream}.
   * @param outputStream {@code OutputStream} to write the WARC record representing this artifact.
   * @return The number of bytes written to the WARC file for this record.
   * @throws IOException
   * @throws HttpException
   */
  public static long writeArtifactData(ArtifactData artifactData, OutputStream outputStream) throws IOException {
    // Get artifact identifier
    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    // Create a WARC record object
    WARCRecordInfo record = new WARCRecordInfo();

    // Mandatory WARC record headers
    record.setRecordId(URI.create(artifactId.getId()));
    record.setCreate14DigitDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));
    record.setType(WARCRecordType.response);

    // Optional WARC record headers
    record.setUrl(artifactId.getUri());
    record.setMimetype("application/http; msgtype=response"); // Content-Type of WARC payload

    // Add LOCKSS-specific WARC headers to record (Note: X-LockssRepo-Artifact-Id and X-LockssRepo-Artifact-Uri are
    // redundant because the same information is recorded as WARC-Record-ID and WARC-Target-URI, respectively).
    record.addExtraHeader(ArtifactConstants.ARTIFACT_ID_KEY, artifactId.getId());
    record.addExtraHeader(ArtifactConstants.ARTIFACT_COLLECTION_KEY, artifactId.getCollection());
    record.addExtraHeader(ArtifactConstants.ARTIFACT_AUID_KEY, artifactId.getAuid());
    record.addExtraHeader(ArtifactConstants.ARTIFACT_URI_KEY, artifactId.getUri());
    record.addExtraHeader(ArtifactConstants.ARTIFACT_VERSION_KEY, String.valueOf(artifactId.getVersion()));

    // We're required to pre-compute the WARC payload size (which is an artifact encoded as an HTTP response stream)
    // but it is not possible to determine the final size without exhausting the InputStream, so we use a
    // DeferredFileOutputStream, copy the InputStream into it, and determine the number of bytes written.
    DeferredTempFileOutputStream dfos = new DeferredTempFileOutputStream((int) DEFAULT_DFOS_THRESHOLD, "writeArtifactData");
    try {
      // Create a HTTP response stream from the ArtifactData
      InputStream httpResponse = ArtifactDataUtil.getHttpResponseStreamFromHttpResponse(
          ArtifactDataUtil.getHttpResponseFromArtifactData(artifactData)
      );

      IOUtils.copy(httpResponse, dfos);

      dfos.flush();
      dfos.close();

      // Set the length of the artifact data
      long contentLength = artifactData.getBytesRead();
      log.debug2("contentLength = {}", contentLength);
      artifactData.setContentLength(contentLength);
      record.addExtraHeader(ArtifactConstants.ARTIFACT_LENGTH_KEY,
          String.valueOf(contentLength));

      // Set content digest of artifact data
      String contentDigest = String.format("%s:%s",
          artifactData.getMessageDigest().getAlgorithm(),
          new String(Hex.encodeHex(artifactData.getMessageDigest().digest())));
      log.debug2("contentDigest = {}", contentDigest);
      artifactData.setContentDigest(contentDigest);
      record.addExtraHeader(ArtifactConstants.ARTIFACT_DIGEST_KEY,
          contentDigest);

      // Attach WARC record payload and set the payload length
      try (InputStream input = dfos.getInputStream()) {
        record.setContentStream(input);
        record.setContentLength(dfos.getByteCount());

        // Write WARCRecordInfo to OutputStream
        CountingOutputStream cout = new CountingOutputStream(outputStream);
        writeWarcRecord(record, cout);

        // Return bytes
        return cout.getCount();
      }
    } finally {
      // Delete the temporary file if one was created
      dfos.deleteTempFile();
    }
  }

  private class UncompressedWARCReader extends WARCReader {
    public UncompressedWARCReader(final String f, final InputStream is) {
      setIn(new CountingInputStream(is));
      initialize(f);
    }

    @Override
    protected WARCRecord createArchiveRecord(InputStream is, long offset) throws IOException {
      return (WARCRecord) currentRecord(new WARCRecord(new SimpleRepositionableStream(is), getReaderIdentifier(), offset, isDigest(), isStrict()));
    }
  }

  /**
   * Creates a WARCRecordInfo object representing a WARC metadata record with a JSON object as its payload.
   *
   * @param refersTo The WARC-Record-Id of the WARC record this metadata is attached to (i.e., for WARC-Refers-To).
   * @param metadata A RepositoryArtifactMetadata with the artifact metadata.
   * @return A WARCRecordInfo representing the given artifact metadata.
   */
  public static WARCRecordInfo createWarcMetadataRecord(String refersTo, RepositoryArtifactMetadata metadata) {
    // Create a WARC record object
    WARCRecordInfo record = new WARCRecordInfo();

    // Set record content stream
    byte[] metadataBytes = metadata.toJson().toString().getBytes();
    record.setContentStream(new ByteArrayInputStream(metadataBytes));

    // Mandatory WARC record headers
    record.setRecordId(URI.create(UUID.randomUUID().toString()));
    record.setCreate14DigitDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));
    record.setType(WARCRecordType.metadata);
    record.setContentLength(metadataBytes.length);
    record.setMimetype(MimeType.JSON);

    // Set the WARC-Refers-To field to the WARC-Record-ID of the artifact
    record.addExtraHeader(WARCConstants.HEADER_KEY_REFERS_TO, refersTo);

    return record;
  }

  /**
   * Writes a WARC info-type record to an {@link OutputStream}.
   *
   * @param output
   * @throws IOException
   */
  public void writeWarcInfoRecord(OutputStream output) throws IOException {
    // Create a WARC record object
    WARCRecordInfo record = new WARCRecordInfo();

    // Mandatory WARC record headers
    record.setRecordId(URI.create(UUID.randomUUID().toString()));
    record.setCreate14DigitDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));
    record.setType(WARCRecordType.response);

    // TODO: Need to discuss with team what kind of information we wish to write and finish this

    // Write WARC info record to WARC file
    /*
    try (OutputStream output = getAppendableOutputStream(warcPath)) {
      writeWarcRecord(record, output);
    }
    */
  }

  /**
   * Writes a WARC record to a given OutputStream.
   *
   * @param record An instance of WARCRecordInfo to write to the OutputStream.
   * @param out    An OutputStream.
   * @throws IOException
   */
  public static void writeWarcRecord(WARCRecordInfo record, OutputStream out) throws IOException {
    // Write the header
    out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

    // Write a CRLF block to separate header from body
    out.write(CRLF_BYTES);

    if (record.getContentStream() != null) {
      // Write the WARC payload
      int bytesWritten = IOUtils.copy(record.getContentStream(), out);

      // Sanity check
      if (bytesWritten != record.getContentLength()) {
        log.warn(
            "Number of bytes written did not match Content-Length header (expected: {} bytes, wrote: {} bytes)",
            record.getContentLength(),
            bytesWritten
        );
      }
    }

    // Write the two CRLF blocks required at end of every record
    out.write(CRLF_BYTES);
    out.write(CRLF_BYTES);
  }

  /**
   * Formats the WARC record ID to a representation that is used
   *
   * @param id
   * @return
   */
  public static String formatWarcRecordId(String id) {
    return String.format("<%s:%s>", WARCID_SCHEME, id);
  }

  /**
   * Composes a String object containing WARC record header of a given WARCRecordInfo.
   *
   * @param record A WARCRecordInfo representing a WARC record.
   * @return The header for this WARCRecordInfo.
   */
  public static String createRecordHeader(WARCRecordInfo record) {
    final StringBuilder sb = new StringBuilder();

    // WARC record identifier
    sb.append(WARC_ID).append(CRLF);

    // WARC record mandatory headers
    sb.append(HEADER_KEY_ID).append(COLON_SPACE).append(formatWarcRecordId(record.getRecordId().toString())).append(CRLF);
//    sb.append(HEADER_KEY_ID).append(COLON_SPACE).append(record.getRecordId().toString()).append(CRLF);
    sb.append(CONTENT_LENGTH).append(COLON_SPACE).append(record.getContentLength()).append(CRLF);
    sb.append(HEADER_KEY_DATE).append(COLON_SPACE).append(record.getCreate14DigitDate()).append(CRLF);
    sb.append(HEADER_KEY_TYPE).append(COLON_SPACE).append(record.getType()).append(CRLF);

    // Optional WARC-Target-URI
    if (!StringUtils.isEmpty(record.getUrl()))
      sb.append(HEADER_KEY_URI).append(COLON_SPACE).append(record.getUrl()).append(CRLF);

    // Optional Content-Type of WARC record payload
    if (record.getContentLength() > 0)
      sb.append(CONTENT_TYPE).append(COLON_SPACE).append(record.getMimetype()).append(CRLF);

    // Extra WARC record headers
    if (record.getExtraHeaders() != null) {
//      record.getExtraHeaders().stream().map(x -> sb.append(x).append(CRLF));

      for (final Iterator<Element> i = record.getExtraHeaders().iterator(); i.hasNext(); ) {
        sb.append(i.next()).append(CRLF);
      }
    }

    return sb.toString();
  }

  /**
   * Creates a warcinfo type WARC record with metadata common to all following WARC records.
   * <p>
   * Adapted from iipc/webarchive-commons.
   *
   * @param headers
   * @param mimeType
   * @param content
   * @return
   */
  public static WARCRecordInfo createWarcInfoRecord(MultiValueMap<String, String> headers, MediaType mimeType, byte[] content) {
    WARCRecordInfo record = new WARCRecordInfo();

    record.setRecordId(URI.create(UUID.randomUUID().toString()));
    record.setType(WARCRecordType.warcinfo);
    record.setCreate14DigitDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));
    record.setContentLength(content == null ? 0 : (long) content.length);
    record.setMimetype(mimeType.toString());

    // Set extra WARC record headers
    if (headers != null) {
      headers.forEach((k, vs) -> vs.forEach(v -> record.addExtraHeader(k, v)));
    }

    // Set content length and input stream
    if (content != null) {
      record.setContentStream(new ByteArrayInputStream(content));
    }

    return record;
  }
}
