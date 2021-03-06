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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.lockss.laaws.rs.core.ArtifactCache;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.CloseCallbackInputStream;
import org.lockss.util.Constants;
import org.lockss.util.concurrent.stripedexecutor.StripedCallable;
import org.lockss.util.concurrent.stripedexecutor.StripedExecutorService;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.storage.StorageInfo;
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
import java.util.stream.Stream;

/**
 * This abstract class aims to capture operations that are common to all {@link ArtifactDataStore} implementations that
 * serialize {@link ArtifactData} as WARC records in a WARC file.
 */
// TODO Get rid of generics
public abstract class WarcArtifactDataStore implements ArtifactDataStore<ArtifactIdentifier, ArtifactData, ArtifactRepositoryState>, WARCConstants {
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
  protected static final String TMP_WARCS_DIR = "tempwarcs";

  protected static final String WARCID_SCHEME = "urn:uuid";
  protected static final String CRLF = "\r\n";
  protected static byte[] CRLF_BYTES;

  public static final Path DEFAULT_BASEPATH = Paths.get("/lockss");
  public final static String DEFAULT_TMPWARCBASEPATH = TMP_WARCS_DIR;

  private static final long DEFAULT_DFOS_THRESHOLD = 16L * FileUtils.ONE_MB;

  protected final static long MAX_AUACTIVEWARCS_RELOADED = 10;

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
  protected Map<CollectionAuidPair, List<Path>> auPathsMap = new HashMap<>();
  protected DataStoreState dataStoreState = DataStoreState.UNINITIALIZED;

  protected ScheduledExecutorService scheduledExecutor;
  protected StripedExecutorService stripedExecutor;
  private ArtifactCache artifactCache;

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

  /**
   * Returns information about the storage size and free space
   *
   * @return A {@code StorageInfo}
   */
  public abstract StorageInfo getStorageInfo();

  protected abstract Path initAuDir(String collectionId, String auid) throws IOException;

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

  protected class ReloadDataStoreStateTask implements Runnable {
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
  protected Path getBasePathFromStorageUrl(URI storageUrl) throws URISyntaxException {
    Path warcPath = Paths.get(storageUrl.getPath());

    return Arrays.stream(getBasePaths())
        .filter(basePath -> warcPath.startsWith(basePath.toString()))
        .sorted(Comparator.reverseOrder()) // Q: Is this right?
        .findFirst()
        .orElse(null);
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
   * Returns a list containing all the paths of this AU.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link List<Path>} containing all paths of this AU.
   */
  public List<Path> getAuPaths(String collectionId, String auid) throws IOException {
    synchronized (auPathsMap) {
      // Get AU's initialized paths from map
      CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);
      List<Path> auPaths = auPathsMap.get(key);

      // Initialize the AU if there is no entry in the map, or return the AU's paths
      // Q: Do we really want to call initAu() here?
      return auPaths == null ? initAu(collectionId, auid) : auPaths;
    }
  }

  /**
   * Returns an active WARC of an AU or initializes a new one, on the base path having the most free space.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @param minSize      A {@code long} containing the minimum available space the underlying base path must have in bytes.
   * @return A {@link Path} containing the path of the chosen active WARC.
   * @throws IOException
   */
  public Path getAuActiveWarcPath(String collectionId, String auid, long minSize) throws IOException {
    synchronized (auActiveWarcsMap) {
      // Get all the active WARCs of this AU
      List<Path> activeWarcs = getAuActiveWarcPaths(collectionId, auid);

      // If there are multiple active WARCs for this AU, pick the one under the base path with the most free space
      Path activeWarc = getMinMaxFreeSpacePath(activeWarcs, minSize);

      // Return the active WARC or initialize a new one if there were no active WARCs or no active WARC resides under a
      // base path with enough space
      return activeWarc == null ? initAuActiveWarc(collectionId, auid, minSize) : activeWarc;
    }
  }

  /**
   * Takes a {@link List} of {@link Paths} and selects the path that has the most available space out of the set of
   * paths meeting a minimum available space threshold.
   *
   * @param paths   A {@link List<Path>} containing the set of paths
   * @param minSize A {@code long} containing the minimum available space threshold in bytes.
   * @return A {@link Path} containing the chosen path among the provided paths or {@code null} if no such path could
   * be found.
   */
  protected Path getMinMaxFreeSpacePath(List<Path> paths, long minSize) {
    if (paths == null) {
      throw new IllegalArgumentException("null paths");
    }

    return paths.stream()
        .filter(p -> getFreeSpace(p.getParent()) > minSize)
        .sorted((a, b) -> (int) (getFreeSpace(b.getParent()) - getFreeSpace(a.getParent())))
        .findFirst()
        .orElse(null);
  }

  /**
   * Returns an array containing all the active WARCs of this AU.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link List<Path>} containing all active WARCs of this AU.
   */
  public List<Path> getAuActiveWarcPaths(String collectionId, String auid) throws IOException {
    synchronized (auActiveWarcsMap) {
      // Get the active WARCs of this AU if it exists in the map
      CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);
      List<Path> auActiveWarcs = auActiveWarcsMap.get(key);

      log.trace("auActiveWarcs = {}", auActiveWarcs);

      if (auActiveWarcs == null) {
        // Reload the active WARCs for this AU
        auActiveWarcs = findAuActiveWarcs(collectionId, auid);
        auActiveWarcsMap.put(key, auActiveWarcs);
      }

      return auActiveWarcs;
    }
  }

  /**
   * In service of {@link WarcArtifactDataStore#findAuActiveWarcs(String, String)}.
   */
  private class WarcSizeThresholdPredicate implements Predicate<Path> {
    @Override
    public boolean test(Path warcPath) {
      try {
        return getWarcLength(warcPath) < getThresholdWarcSize();
      } catch (IOException e) {
        log.warn("Caught IOException", e);
        return false;
      }
    }
  }

  /**
   * In service of {@link WarcArtifactDataStore#findAuActiveWarcs(String, String)}.
   */
  private class WarcLengthComparator implements Comparator<Path> {
    @Override
    public int compare(Path a, Path b) {
      try {
        return Long.compare(getWarcLength(a), getWarcLength(b));
      } catch (IOException e) {
        log.warn("Caught IOException", e);
        return Integer.MIN_VALUE;
      }
    }
  }

  /**
   * In service of {@link WarcArtifactDataStore#findAuActiveWarcs(String, String)}.
   */
  private Collection<Path> findWarcsOrEmpty(Path path) {
    try {
      return findWarcs(path);
    } catch (IOException e) {
      log.warn("Caught IOException", e);
      return Collections.EMPTY_LIST;
    }
  }

  /**
   * Finds the artifact-containing WARCs of an AU that have not met size or block-usage thresholds and are therefore
   * eligible to be reloaded as active WARCs (to have new artifacts appended to the WARC).
   *
   * @param collectionId A {@link String} containing the collection ID.
   * @param auid         A {@link String} containing the the AUID.
   * @return A {@link List<Path>} containing paths to WARCs that are eligible to be reloaded as active WARCs.
   * @throws IOException
   */
  protected List<Path> findAuActiveWarcs(String collectionId, String auid) throws IOException {
    return findAuArtifactWarcsStream(collectionId, auid)
        .filter(new WarcSizeThresholdPredicate())
        .sorted(new WarcLengthComparator())
        .limit(MAX_AUACTIVEWARCS_RELOADED)
        .collect(Collectors.toList());
  }

  protected List<Path> findAuArtifactWarcs(String collectionId, String auid) throws IOException {
    return findAuArtifactWarcsStream(collectionId, auid).collect(Collectors.toList());
  }

  protected Stream<Path> findAuArtifactWarcsStream(String collectionId, String auid) throws IOException {
    return getAuPaths(collectionId, auid).stream()
        .map(auPath -> findWarcsOrEmpty(auPath))
        .flatMap(Collection::stream)
        .filter(warcPath -> warcPath.getFileName().toString().startsWith("artifacts_"));
  }

  /**
   * Returns the path of a journal of an AU on the given base path.
   *
   * @param basePath    A {@link Path} containing a repository base path.
   * @param journalName A {@link String} containing the name of the journal.
   * @return A {@link Path} containing the path to the journal.
   */
  protected Path getAuJournalPath(Path basePath, String collection, String auid, String journalName) {
    return getAuPath(basePath, collection, auid).resolve(journalName + "." + WARC_FILE_EXTENSION);
  }

  /**
   * Returns the path to a journal of an AU across all the configured data store base paths.
   *
   * @param journalName A {@link String} containing the name of the journal.
   * @return A {@link Path[]} containing the paths to the journal on across all the configured data store base paths.
   */
  protected Path[] getAuJournalPaths(String collection, String auid, String journalName) throws IOException {
    return getAuPaths(collection, auid).stream()
        .map(auPath -> auPath.resolve(journalName + "." + WARC_FILE_EXTENSION))
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
  public URI makeWarcRecordStorageUrl(Path filePath, long offset, long length) {
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
  protected static String generateActiveWarcName(String collectionId, String auid) {
    ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("UTC"));
    return generateActiveWarcName(collectionId, auid, zdt);
  }

  protected static String generateActiveWarcName(String collectionId, String auid, ZonedDateTime zdt) {
    String timestamp = zdt.format(FMT_TIMESTAMP);
    String auidHash = DigestUtils.md5Hex(auid);
    return String.format("artifacts_%s-%s_%s.warc", collectionId, auidHash, timestamp);
  }

  /**
   * Initializes a new active WARC for an AU on the base path with the most free space.
   *
   * @param collectionId A {@link String} containing the name of the collection the AU belongs to.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @param minSize      A {@code long} containing the minimum amount of available space the underlying filesystem must
   *                     have available for the new active WARC, in bytes.
   * @return The {@link Path} to the new active WARC for this AU.
   * @throws IOException
   */
  public Path initAuActiveWarc(String collectionId, String auid, long minSize) throws IOException {
    // Debugging
    log.trace("collection = {}", collectionId);
    log.trace("auid = {}", auid);
    log.trace("minSize = {}", minSize);

    // Get an array of the AU's initialized paths in storage
    List<Path> auPaths = getAuPaths(collectionId, auid);

    // Determine which existing AU path to use based on currently available space
    Path auPath = getMinMaxFreeSpacePath(auPaths, minSize);

    if (auPath == null) {
      //// AU not initialized or no existing AU meets minimum space requirement

      // Have we exhausted all available base paths?
      if (auPaths.size() < basePaths.length) {
        // Create a new AU base directory (or get the existing one with the most available space)
        auPath = initAuDir(collectionId, auid);
      } else {
        log.error("No AU directory available: Configured data store base paths are full");
        throw new IOException("No AU directory available");
      }
    }

    // Generate path to new active WARC file under chosen AU path
    Path auActiveWarc = auPath.resolve(generateActiveWarcName(collectionId, auid));

    // Add new active WARC to active WARCs map
    synchronized (auActiveWarcsMap) {
      // Initialize the new WARC file
      initWarc(auActiveWarc);

      // Add WARC file path to list of active WARC paths of this AU
      CollectionAuidPair key = new CollectionAuidPair(collectionId, auid);
      List<Path> auActiveWarcs = auActiveWarcsMap.getOrDefault(key, new ArrayList<>());
      auActiveWarcs.add(auActiveWarc);
      auActiveWarcsMap.put(key, auActiveWarcs);
    }

    return auActiveWarc;
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
          log.warn("Attempted to seal an active WARC of an AU that is not active!");
        }

        auActiveWarcsMap.put(key, activeWarcs);
      } else {
        log.warn("Attempted to seal an active WARC of an AU having no active WARCs!");
      }
    }
  }

  // *******************************************************************************************************************
  // * TEMPORARY WARCS LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Submits a {@link GarbageCollectTempWarcsTask} for execution by the {@link ScheduledExecutorService}.
   */
  protected void runGarbageCollector() {
    scheduledExecutor.submit(new GarbageCollectTempWarcsTask());
  }

  protected class GarbageCollectTempWarcsTask implements Runnable {
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
        garbageCollectTempWarc(tmpWarc);
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
   * Removes a temporary WARC if it is not in use and eligible to be deleted (as determined by
   * {@link WarcArtifactDataStore#isTempWarcRemovable(Path)}.
   *
   * @param tmpWarcPath A {@link Path} containing the path to the temporary WARC under consideration.
   */
  protected void garbageCollectTempWarc(Path tmpWarcPath) {
    WarcFile tmpWarcFile = null;

    log.trace("Processing [tmpWarc = {}]", tmpWarcPath);

    // Remove the temporary WARC from the pool if it is active there
    synchronized (tmpWarcPool) {
      if (tmpWarcPool.isInUse(tmpWarcPath) || TempWarcInUseTracker.INSTANCE.isInUse(tmpWarcPath)) {
        // Temporary WARC is in use - skip it
        log.debug2("Temporary WARC file is in use; will attempt a GC again later");
        return;

      } else if (tmpWarcPool.isInPool(tmpWarcPath)) {
        // Temporary WARC is a member of the pool but not currently in use - process it
        tmpWarcFile = tmpWarcPool.removeWarcFile(tmpWarcPath);

        if (tmpWarcFile == null) {
          // This message is worth paying attention to if logged - it may indicate a problem with synchronization
          log.error("Could not remove temporary WARC file [tmpWarc: {}]", tmpWarcPath);
          return;
        }

      } else {
        log.warn("Temporary WARC is not a member of the pool of temporary WARcs [tmpWarc: {}]", tmpWarcPath);
      }
    }

    // Determine whether this temporary WARC should be removed
    try {
      // Mark the WARC as in-use by this GC thread
      TempWarcInUseTracker.INSTANCE.markUseStart(tmpWarcPath);

      if (isTempWarcRemovable(tmpWarcPath)) {
        // Yes: Remove the temporary WARC from storage
        log.debug("Removing temporary WARC file [tmpWarc: {}]", tmpWarcPath);

        // Synchronized because result from getUseCount(Path) is not thread-safe
        synchronized (TempWarcInUseTracker.INSTANCE) {
          // Get temporary WARC use count
          long useCount = TempWarcInUseTracker.INSTANCE.getUseCount(tmpWarcPath);

          if (useCount == 1) {
            // Remove temporary WARC
            removeWarc(tmpWarcPath);
          } else if (useCount > 1) {
            // Temporary WARC still in use elsewhere
            log.debug("Temporary WARC still in use; not removing [tmpWarcPath: {}]", tmpWarcPath);
          } else {
            // This should never happen
            log.error("Unexpected use count! [useCount: {}]", useCount);
            throw new IllegalStateException("Unexpected use count!");
          }
        }
      } else {
        // No: Return the temporary WARC to the pool if we removed it from the pool earlier
        if (tmpWarcFile != null) {
          synchronized (tmpWarcPool) {
            log.debug("Returning {} to temporary WARC pool", tmpWarcFile.getPath());
            tmpWarcPool.returnWarcFile(tmpWarcFile);
          }
        }
      }
    } catch (IOException e) {
      log.error(
          "Caught IOException while trying to GC temporary WARC file [tmpWarcPath: {}]",
          tmpWarcPath,
          e
      );
    } finally {
      // Mark WARC use ended by GC
      TempWarcInUseTracker.INSTANCE.markUseEnd(tmpWarcPath);
    }
  }

  /**
   * Reloads the temporary WARCs under all the base paths configured in this data store.
   *
   * @throws IOException
   */
  public void reloadTemporaryWarcs() throws IOException {
    if (artifactIndex == null) {
      throw new IllegalStateException("Cannot reload data store state from temporary WARCs without an artifact index");
    }

    for (Path tmpWarcBasePath : getTmpWarcBasePaths()) {
      reloadTemporaryWarcs(artifactIndex, tmpWarcBasePath);
    }
  }

  /**
   * Reads and reloads state from temporary WARCs, including the requeuing of copy tasks of committed artifacts from
   * temporary to permanent storage. Removes temporary WARCs if eligible:
   * <p>
   * A temporary WARC may be removed if all the records contained within it are the serializations of artifacts that are
   * either uncommitted-but-expired, committed-and-moved-to-permanent-storage, or deleted.
   */
  protected void reloadTemporaryWarcs(ArtifactIndex index, Path tmpWarcBasePath) throws IOException {
    log.info("Reloading temporary WARCs from {}", tmpWarcBasePath);

    Collection<Path> tmpWarcs = findWarcs(tmpWarcBasePath);

    log.debug("Found {} temporary WARCs: {}", tmpWarcs.size(), tmpWarcs);

    // Iterate over the temporary WARC files that were found
    for (Path tmpWarc : tmpWarcs) {
      // Q: Use file lock to prevent other processes or threads from modifying file?
      reloadOrRemoveTemporaryWarc(index, tmpWarc);
    }

    // Reset maps
    // FIXME Revisit - these have the potential of growing very large
    storageUrls = new HashMap<>();
    artifactStates = new HashMap<>();

    log.debug("Finished reloading temporary WARCs from {}", tmpWarcBasePath);
  }

  protected void reloadOrRemoveTemporaryWarc(ArtifactIndex index, Path tmpWarc) throws IOException {
    log.trace("tmpWarc = {}", tmpWarc);

    // Q: In what situation can this happen?
    if (tmpWarcPool.isInPool(tmpWarc)) {
      log.debug("Temporary WARC already in pool [tmpWarc: {}]", tmpWarc);
      return;
    }

    // ********************************************************
    // Determine whether all records in this WARC are removable
    // ********************************************************

    boolean isWarcFileRemovable = true;

    // Open WARC file
    try (InputStream warcStream = markAndGetInputStream(tmpWarc)) {

      // Get an ArchiveReader (an implementation of Iterable) over ArchiveRecord objects
      ArchiveReader archiveReader = new UncompressedWARCReader(tmpWarc.toString(), warcStream);
      archiveReader.setDigest(false);

      // Iterate over the WARC records
      for (ArchiveRecord record : archiveReader) {
        boolean isRecordRemovable = false;

        ArtifactIdentifier aid = ArtifactDataFactory.buildArtifactIdentifier(record.getHeader());

        ArtifactState artifactState = getArtifactState(aid, isArtifactExpired(record));

        log.trace("artifact.state = {}", artifactState);

        // Resume artifact lifecycle based on the artifact's state
        switch (artifactState) {
          case NOT_INDEXED:
            // An artifact was found in a temporary WARC that is not indexed for some reason - index the artifact now
            log.debug("Artifact missing from index; indexing now [artifactId: {}]", aid.getId());

            // Construct an ArtifactData from reading the WARC record
            ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

            // Note: ArchiveRecordHeader#getLength() does not account for the pair of CRLFs at the end of every WARC
            // record. We correct that here:
            long recordLength = record.getHeader().getLength() + 4L;

            if (!isArtifactCommitted(aid)) {
              // Set the artifact's storage URL to the temporary WARC record we read it from:
              artifactData.setStorageUrl(makeWarcRecordStorageUrl(tmpWarc, record.getHeader().getOffset(), recordLength));
            } else {
              if (!storageUrls.containsKey(aid.getId())) {
                // Get storage URLs of artifacts in this AU
                getAuArtifactStorageUrls(aid.getCollection(), aid.getAuid());
              }

              // Set storage URL to the WARC record in permanent storage
              artifactData.setStorageUrl(storageUrls.get(aid.getId()));
            }

            // Create artifact repository state from journal
            ArtifactRepositoryState state = new ArtifactRepositoryState(
                aid,
                isArtifactCommitted(aid),
                isArtifactDeleted(aid)
            );

            // Set artifact data's repository state (from AU journal)
            artifactData.setArtifactRepositoryState(state);

            // Index the artifact
            index.indexArtifact(artifactData);

            // Fall-through to COMMITTED

          case COMMITTED:
            // Requeue the copy of this artifact from temporary to permanent storage
            if (isArtifactCommitted(aid)) {
              try {
                Artifact artifact = index.getArtifact(aid.getId());

                // Only reschedule a copy to permanent storage if the artifact is still in temporary storage
                // according to the artifact index
                if (isTmpStorage(getPathFromStorageUrl(new URI(artifact.getStorageUrl())))) {
                  log.debug("Re-queuing move to permanent storage for artifact [artifactId: {}]", aid.getId());

                  // TODO Rename this task and remove second mark-as-committed
                  stripedExecutor.submit(new CommitArtifactTask(artifact));
                }
              } catch (RejectedExecutionException e) {
                log.warn("Could not re-queue copy of artifact to permanent storage [artifactId: {}]", aid.getId(), e);
              } catch (URISyntaxException e) {
                // This should never happen
                log.error("Bad storage URL [artifactId: {}]", aid.getId());
                break;
              }
            }

          case UNCOMMITTED:
            // Nothing to do
            log.debug2(
                "WARC not removable [artifactId: {}, state: {}, isCommitted: {}]",
                aid.getId(), artifactState, isArtifactCommitted(aid)
            );

            break;

          case EXPIRED:
          case DELETED:
            // Remove artifact reference from index if it exists
            if (index.deleteArtifact(aid.getId())) {
              log.debug2("Removed artifact from index [artifactId: {}]", aid.getId());
            }

            // Fall-through to COPIED

          case COPIED:
            log.trace("Temporary WARC record is removable [warcId: {}, state: {}]",
                record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID), artifactState);

            // Mark this WARC record as removable
            isRecordRemovable = true;
            break;

          case UNKNOWN:
            // TODO Introduce more robustness

          default:
            log.warn("Could not determine artifact state; aborting reload [artifactId: {}]", aid.getId());
            break;
        }

        // All records must be removable for temporary WARC file to be removable
        isWarcFileRemovable &= isRecordRemovable;
      }
    } catch (IOException e) {
      log.error("Could not reload temporary WARC [tmpWarc: {}]", tmpWarc);
      throw e;
    }

    // ****************************************
    // Handle the result of isWarcFileRemovable
    // ****************************************

    // Q: Where else would it be in use?
    if (isWarcFileRemovable && !TempWarcInUseTracker.INSTANCE.isInUse(tmpWarc)) {
      try {
        log.debug("Removing temporary WARC [tmpWarc: {}]", tmpWarc);
        removeWarc(tmpWarc);
      } catch (IOException e) {
        log.warn("Could not remove removable temporary WARC [tmpWarc: {}]", tmpWarc, e);
      }
    } else {
      // WARC file still in use; add it to the temporary WARC pool
      long tmpWarcFileLen = getWarcLength(tmpWarc);
      tmpWarcPool.addWarcFile(new WarcFile(tmpWarc, tmpWarcFileLen));
    }
  }

  // Artifact ID to artifact storage URL
  Map<String, URI> storageUrls = new HashMap<>();

  /**
   * Returns a map from artifact ID to its storage URL.
   *
   * @param collection A {@link String} containing the collection ID.
   * @param auid A {@link String} containing the Archival Unit ID (AUID).
   * @return A {@link Map<String, URI>} containing a map from artifact ID to storage URL.
   * @throws IOException
   */
  protected Map<String, URI> getAuArtifactStorageUrls(String collection, String auid) throws IOException {
    // Process all WARCs in the AU, across all its paths
    for (Path warcPath : findAuArtifactWarcs(collection, auid)) {

      // Open WARC and get an ArchiveReader
      try (InputStream warcStream = markAndGetInputStream(warcPath)) {
        // FIXME This is expensive because UncompressedWARCReader uses skip() which reads data
        ArchiveReader warcReader = new UncompressedWARCReader(warcPath.toString(), warcStream);
      warcReader.setDigest(false);

        // Process WARC records
        for (ArchiveRecord warcRecord : warcReader) {

          // Read record header
          ArchiveRecordHeader warcHeader = warcRecord.getHeader();
          ArtifactIdentifier artifactId = ArtifactDataFactory.buildArtifactIdentifier(warcHeader);

          // Note: ArchiveRecordHeader#getLength() does not account for the pair of CRLFs at the end of every WARC
          // record. We correct that here:
          URI storageUrl = makeWarcRecordStorageUrl(warcPath, warcHeader.getOffset(), warcHeader.getLength() + 4L);

          if (storageUrls.containsKey(artifactId.getId())) {
            log.warn("Artifact found in multiple locations [artifactId: {}]", artifactId);
            log.debug2("storageUrl.prev = {}", storageUrls.get(artifactId.getId()));
            log.debug2("storageUrl.next = {}", storageUrl);
          }

          // Add to map
          storageUrls.put(artifactId.getId(), storageUrl);
        }
      }
    }

    return storageUrls;
  }

  /**
   * Determines whether a temporary WARC file is removable.
   * <p>
   * A temporary WARC file is removable if all of the WARC records contained within it may be removed.
   * <p>
   * Note: This is in service of the temporary WARC garbage collector. This is slightly different from reloading
   * temporary WARCs, which may resume the artifact's lifecycle depending on its state (e.g., re-queuing a copy).
   *
   * @param tmpWarc A {@link Path} containing the path to a temporary WARC file.
   * @return A {@code boolean} indicating whether the temporary WARC may be removed.
   * @throws IOException
   */
  protected boolean isTempWarcRemovable(Path tmpWarc) throws IOException {
    try (InputStream warcStream = markAndGetInputStream(tmpWarc)) {
      // Get a WARCReader to the temporary WARC
      ArchiveReader archiveReader = WARCReaderFactory.get(tmpWarc.toString(), warcStream, true);
      archiveReader.setDigest(false);

      for (ArchiveRecord record : archiveReader) {
        if (!isTempWarcRecordRemovable(record)) {
          // Temporary WARC contains a WARC record that is still needed
          return false;
        }
      }

      // All records in this temporary WARC file are removable so the file is removable
      return true;
    }
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
  protected boolean isTempWarcRecordRemovable(ArchiveRecord record) {
    // Get WARC record headers
    ArchiveRecordHeader headers = record.getHeader();

    // Get artifact identifier from WARC header
    ArtifactIdentifier aid = ArtifactDataFactory.buildArtifactIdentifier(headers);

    // Get the WARC type
    String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

    switch (WARCRecordType.valueOf(recordType)) {
      case response:
      case resource:
        try {
          switch (getArtifactState(aid, isArtifactExpired(record))) {
            case UNKNOWN:
            case NOT_INDEXED:
            case COMMITTED:
            case UNCOMMITTED:
              return false;

            default:
              // Expired, deleted, copied
              return true;
          }
        } catch (IOException e) {
          log.error("Could not determine artifact state", e);
          return false;
        }

      default:
        // All other WARC record types may be removed
        return true;
    }
  }

  // *******************************************************************************************************************
  // * ARTIFACT LIFECYCLE
  // *******************************************************************************************************************

  /**
   * Returns the {@link ArtifactState} of an artifact.
   */
  protected ArtifactState getArtifactState(ArtifactIdentifier aid, boolean isExpired) throws IOException {
    if (aid == null) {
      throw new IllegalArgumentException("Null ArtifactIdentifier");
    }

    // ********************************
    // Determine if artifact is deleted
    // ********************************

    try {
      if (isArtifactDeleted(aid)) {
        return ArtifactState.DELETED;
      }
    } catch (IOException e) {
      log.warn("Could not determine artifact state", e);
      return ArtifactState.UNKNOWN;
    }

    // ********************************
    // Get artifact from cache or index
    // ********************************

    Artifact artifact = null;

    // Check the artifact cache if one is available
    if (artifactCache != null) {
      artifact = artifactCache.get(aid.getCollection(), aid.getAuid(), aid.getUri(), aid.getVersion());
    }

    if (artifact == null) {
      // No artifact cache or cache miss: Retrieve artifact from the index
      artifact = artifactIndex.getArtifact(aid);

      // Add it to the artifact cache if available
      if (artifactCache != null) {
        artifactCache.put(artifact);
      }
    }

    // ************************
    // Determine artifact state
    // ************************

    if (artifact != null) {
      try {
        if (artifact.isCommitted() && !isTmpStorage(getPathFromStorageUrl(new URI(artifact.getStorageUrl())))) {
          // Artifact is marked committed and in permanent storage
          return ArtifactState.COPIED;
        } else if (artifact.isCommitted()) {
          // Artifact is marked committed but not copied to permanent storage
          return ArtifactState.COMMITTED;
        } else if (!artifact.isCommitted() && !isExpired) {
          // Uncommitted and not copied but indexed
          return ArtifactState.UNCOMMITTED;
        }
      } catch (URISyntaxException e) {
        // This should never happen; storage URLs are generated internally
        log.error("Bad storage URL: [artifact: {}]", artifact, e);
        return ArtifactState.UNKNOWN;
      }
    }

    if (isExpired) {
      return ArtifactState.EXPIRED;
    }

    return ArtifactState.NOT_INDEXED;
  }

  /**
   * Returns a boolean indicating whether the an artifact is expired by reading the headers of its WARC record.
   *
   * @param record The {@link ArchiveRecord} instance of the artifact.
   * @return A {@code boolean} indicating whether the artifact is expired.
   */
  protected boolean isArtifactExpired(ArchiveRecord record) {
    // Get WARC record headers
    ArchiveRecordHeader headers = record.getHeader();

    // Parse WARC-Date field and determine if this record / artifact is expired (same date should be in index)
    String warcDateHeader = headers.getDate();
    Instant created = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(warcDateHeader));
    Instant expiration = created.plus(getUncommittedArtifactExpiration(), ChronoUnit.MILLIS);
    return Instant.now().isAfter(expiration);
  }

  /**
   * Returns a boolean indicating whether an artifact is marked as deleted in the journal.
   *
   * @param aid The {@link ArtifactIdentifier} of the artifact to check.
   * @return A {@code boolean} indicating whether the artifact is marked as deleted.
   * @throws IOException
   */
  protected boolean isArtifactDeleted(ArtifactIdentifier aid) throws IOException {
    ArtifactRepositoryState state = getArtifactRepositoryState(aid);

    if (state != null) {
      // YES: Repository metadata journal entry found for this artifact
      return state.isDeleted();
    }

    throw new LockssNoSuchArtifactIdException();
  }

  /**
   * Returns a boolean indicating whether an artifact is marked as committed in the journal.
   *
   * @param aid The {@link ArtifactIdentifier} of the artifact to check.
   * @return A {@code boolean} indicating whether the artifact is marked as committed.
   * @throws IOException
   */
  protected boolean isArtifactCommitted(ArtifactIdentifier aid) throws IOException {
    ArtifactRepositoryState state = getArtifactRepositoryState(aid);

    if (state != null) {
      // YES: Repository metadata journal entry found for this artifact
      return state.isCommitted();
    }

    throw new LockssNoSuchArtifactIdException();
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

    if (basePaths.length <= 0) {
      throw new IllegalStateException("No data store base paths configured");
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
          .max((a, b) -> (int) (getFreeSpace(b) - getFreeSpace(a)))
          .filter(bp -> getFreeSpace(bp) >= recordLength)
          .orElse(null);

      if (basePath == null) {
        // Could also be null if there are no base paths but we checked that earlier
        log.error("No base path available with enough space for this new artifact");
        throw new IOException("No space left");
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
        try (InputStream input = dfos.getDeleteOnCloseInputStream()) {

          // Write the serialized artifact to the temporary WARC file
          bytesWritten = IOUtils.copyLarge(input, output);

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

      // Update ArtifactData object with new properties
      artifactData.setArtifactRepositoryState(new ArtifactRepositoryState(artifactId, false, false));
      artifactData.setStorageUrl(makeWarcRecordStorageUrl(tmpWarcFilePath, offset, recordLength));

      // **********************************
      // Write artifact metadata to journal
      // **********************************

      // Write journal entry to journal file under an existing AU path
      List<Path> auPaths = getAuPaths(artifactId.getCollection(), artifactId.getAuid());

      Path auPath = auPaths.stream()
          .sorted((a,b) -> (int) (getFreeSpace(b) - getFreeSpace(a)))
          .findFirst()
          .orElse(null); // should never happen

      Path auBasePath = Arrays.stream(getBasePaths())
          .sorted()
          .filter(bp -> auPath.startsWith(bp))
          .findFirst()
          .orElse(null); // should never happen

      // Write journal entry
      updateArtifactRepositoryState(auBasePath, artifactId, artifactData.getArtifactRepositoryState());

      // ******************
      // Index the artifact
      // ******************

      artifactIndex.indexArtifact(artifactData);

      // *******************
      // Return the artifact
      // *******************

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
      log.error("Could not add artifact data!", e);
      throw e;
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
        log.debug("Artifact not found: [artifactId: {}]", artifactId);
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
      WARCRecord warcRecord = new WARCRecord(warcStream, getClass().getSimpleName(), 0L, false, false);

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
      artifactData.setArtifactRepositoryState(getArtifactRepositoryState(artifact.getIdentifier()));

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

    log.trace("artifact = {}", artifact);

    String artifactId = artifact.getId();

    // Guard against deleted or non-existent artifact
    if (!artifactIndex.artifactExists(artifactId) || isArtifactDeleted(artifact.getIdentifier())) {
      log.debug("Artifact not found: [artifactId: {}]", artifactId);
      return null;
    }

    try {
      // FIXME
      ArtifactData ad = getArtifactData(artifact);

      // Determine if the artifact is expired
      Instant created = Instant.ofEpochMilli(ad.getStoredDate());
      Instant expiration = created.plus(getUncommittedArtifactExpiration(), ChronoUnit.MILLIS);
      boolean isExpired = Instant.now().isAfter(expiration);

      ad.release();

      // Determine what action to take based on the state of the artifact
      // FIXME: Potential for race condition? What if the state of the artifact changes?
      ArtifactState artifactState = getArtifactState(artifact.getIdentifier(), isExpired);
      switch (artifactState) {
        case NOT_INDEXED:
          // We have an artifact so this should be recoverable
          log.warn("Artifact missing from index; adding and continuing [artifactId: {}]", artifact.getId());
          artifactIndex.indexArtifact(getArtifactData(artifact));

        case UNCOMMITTED:
          // Mark artifact as committed in the index
          artifactIndex.commitArtifact(artifact.getId());
          artifact.setCommitted(true);

          // Record new committed state to artifact data repository metadata journal
          ArtifactRepositoryState artifactRepoState =
              new ArtifactRepositoryState(artifact.getIdentifier(), true, false);

          Path basePath = getBasePathFromStorageUrl(new URI(artifact.getStorageUrl()));
          updateArtifactRepositoryState(basePath, artifact.getIdentifier(), artifactRepoState);

        case COMMITTED:
          // Submit the task to copy the artifact data from temporary to permanent storage
          return stripedExecutor.submit(new CommitArtifactTask(artifact));

        case COPIED:
          // This artifact is already marked committed and is in permanent storage. Wrap in Future and return it.
          return new CompletedFuture<>(artifact);

        case EXPIRED:
        case DELETED:
          log.warn("Cannot commit deleted or expired artifact [artifactId: {}, state: {}]", artifact.getId(), artifactState.toString());
          break;

        case UNKNOWN:
        default:
          log.error("Unknown artifact state; cannot commit [artifactId: {}, state: {}]", artifact.getId(),
              artifactState.toString());
      }
    } catch (URISyntaxException e) {
      // This should never happen since storage URLs are internal
      throw new IllegalStateException(e);
    }

    return null;
  }

  /**
   * Implementation of {@link Callable} that commits an artifact from temporary to permanent storage.
   * <p>
   * This is implemented as a {@link StripedCallable} because we maintain one active WARC file per AU in which to commit
   * artifacts permanently.
   */
  protected class CommitArtifactTask implements StripedCallable<Artifact> {
    protected Artifact artifact;

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
     *
     * @return
     * @throws Exception
     */
    @Override
    public Artifact call() throws Exception {
      // Get the temporary WARC record location from the artifact's storage URL
      WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(new URI(artifact.getStorageUrl()));
      long recordOffset = loc.getOffset();
      long recordLength = loc.getLength();

      // Get an active WARC of this AU to append the artifact to
      Path dst = getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid(), recordLength);

      // Artifact will be appended as a WARC record to this WARC file so its offset is the current length of the file
      long warcLength = getWarcLength(dst);

      // *********************************
      // Append WARC record to active WARC
      // *********************************

      try (OutputStream output = getAppendableOutputStream(dst)) {
        try (InputStream is = markAndGetInputStreamAndSeek(loc.getPath(), loc.getOffset())) {
          long bytesWritten = StreamUtils.copyRange(is, output, 0, recordLength - 1);

          log.debug2("Copied artifact {}: Wrote {} of {} bytes starting at byte offset {} to {}; size of WARC file is" +
                  " now {}",
              artifact.getIdentifier().getId(),
              bytesWritten,
              recordLength,
              warcLength,
              dst,
              warcLength + recordLength
          );
        }
      }

      // Seal if we've gone over the size threshold
      if (warcLength + recordLength >= getThresholdWarcSize()) {
        sealActiveWarc(artifact.getCollection(), artifact.getAuid(), dst);
      }

      // ******************
      // Update storage URL
      // ******************

      // Set the artifact's new storage URL and update the index
      artifact.setStorageUrl(makeWarcRecordStorageUrl(dst, warcLength, recordLength).toString());
      artifactIndex.updateStorageUrl(artifact.getId(), artifact.getStorageUrl());

      log.debug2("Updated storage URL [artifactId: {}, storageUrl: {}]",
          artifact.getId(), artifact.getStorageUrl()
      );

      // *********************
      // Update artifact state
      // *********************

      Path basePath = getBasePathFromStorageUrl(new URI(artifact.getStorageUrl()));
      ArtifactRepositoryState state = new ArtifactRepositoryState(artifact.getIdentifier(), true, false);
      updateArtifactRepositoryState(basePath, artifact.getIdentifier(), state);

      artifact.setCommitted(true);

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
      ArtifactRepositoryState state = getArtifactRepositoryState(artifact.getIdentifier());

      if (!state.isDeleted()) {
        // Set deleted flag
        state.setDeleted(true);

        // Write new state to artifact data repository metadata journal
        updateArtifactRepositoryState(
            getBasePathFromStorageUrl(new URI(artifact.getStorageUrl())),
            artifact.getIdentifier(),
            state
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
  @Deprecated
  protected InputStream getInputStreamFromStorageUrl(URI storageUrl) throws IOException {
    WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(storageUrl);
    return getInputStreamAndSeek(loc.getPath(), loc.getOffset());
  }

  // *******************************************************************************************************************
  // * INNER CLASSES
  // *******************************************************************************************************************

  // TODO - Pull this out and along WarcFile?
  protected static class WarcRecordLocation {
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
      replayArtifactRepositoryStateJournal(index, metadataFile);
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
  protected void reindexArtifactsFromWarcs(ArtifactIndex index, Collection<Path> artifactWarcFiles) throws IOException {
    // Reindex artifacts from temporary and permanent storage
    for (Path warcFile : artifactWarcFiles) {
      reindexArtifactsFromWarc(index, warcFile);
    }
  }

  protected void reindexArtifactsFromWarc(ArtifactIndex index, Path warcFile) throws IOException {
    boolean isWarcInTemp = isTmpStorage(warcFile);

    try (InputStream warcStream = markAndGetInputStream(warcFile)) {
      for (ArchiveRecord record : new UncompressedWARCReader("WarcArtifactDataStore", warcStream)) {
        log.debug(
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
              log.debug("Artifact is already indexed [artifactId: {}]", artifactData.getIdentifier().getId());
              continue;
            }

            // ArchiveRecordHeader#getLength() does not include the pair of CRLFs at the end of every WARC record so
            // we add four bytes to the length
            long recordLength = record.getHeader().getLength() + 4L;

            // Set ArtifactData storage URL
            artifactData.setStorageUrl(makeWarcRecordStorageUrl(warcFile, record.getHeader().getOffset(), recordLength));

            // Default repository metadata for all ArtifactData objects to be indexed
            artifactData.setArtifactRepositoryState(new ArtifactRepositoryState(
                artifactData.getIdentifier(),
                !isWarcInTemp,
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

  // *******************************************************************************************************************
  // * JOURNAL OPERATIONS
  // *******************************************************************************************************************

  /**
   * Updates the repository metadata of artifact by appending an entry to the repository metadata journal in the
   * artifact's AU.
   *
   * @param basePath         A {@link Path} containing the base path of the artifact.
   * @param artifactId       The {@link ArtifactIdentifier} of the artifact to update.
   * @param state The new {@link ArtifactRepositoryState}.
   * @return The {@link ArtifactRepositoryState} that was recorded.
   * @throws IOException
   */
  // TODO: Generalize this to arbitrary metadata
  public synchronized ArtifactRepositoryState updateArtifactRepositoryState(
      Path basePath,
      ArtifactIdentifier artifactId,
      ArtifactRepositoryState state
  ) throws IOException {

    Objects.requireNonNull(artifactId, "Artifact identifier is null");
    Objects.requireNonNull(state, "Repository artifact metadata is null");

    Path auJournalPath = getAuJournalPath(basePath, artifactId.getCollection(), artifactId.getAuid(),
        ArtifactRepositoryState.LOCKSS_JOURNAL_ID);

    log.trace("artifactId = {}", artifactId);
    log.trace("auJournalPath = {}", auJournalPath);
    log.trace("state = {}", state.toJson());

    // Initialize journal WARC file
    initWarc(auJournalPath);

    try (OutputStream output = getAppendableOutputStream(auJournalPath)) {
      // Append WARC metadata record to the journal
      WARCRecordInfo metadataRecord = createWarcMetadataRecord(artifactId.getId(), state);
      writeWarcRecord(metadataRecord, output);
      output.flush();

      artifactStates.put(artifactId.getId(), state);
    }

    return state;
  }

  /**
   * Truncates a journal by rewriting it with only its most recent entry per artifact ID.
   *
   * @param journalPath A {@link Path} containing the path to the data store journal to truncate.
   * @throws IOException
   */
  protected void truncateAuJournalFile(Path journalPath) throws IOException {
    // Get latest entry per artifact ID
    List<ArtifactRepositoryState> journalEntries = readAuJournalEntries(journalPath, ArtifactRepositoryState.class); // FIXME

    // Replace the journal with a new file
    removeWarc(journalPath); // FIXME
    initWarc(journalPath);

    // Write journal with only latest entries
    try (OutputStream output = getAppendableOutputStream(journalPath)) {
      for (ArtifactRepositoryState journalEntry: journalEntries) {
        // Append WARC metadata record to the new journal
        WARCRecordInfo metadataRecord = createWarcMetadataRecord(journalEntry.getArtifactId(), journalEntry);
        writeWarcRecord(metadataRecord, output);
        output.flush();
      }
    }
  }

  Map<String, ArtifactRepositoryState> artifactStates = new HashMap<>();

  /**
   * Reads an artifact's current repository state from storage.
   *
   * @param aid An {@link ArtifactIdentifier}
   * @return The {@link ArtifactRepositoryState} of the artifact.
   * @throws IOException
   */
  protected ArtifactRepositoryState getArtifactRepositoryState(ArtifactIdentifier aid) throws IOException {
    if (aid == null) {
      throw new IllegalArgumentException("Null artifact identifier");
    }

    // Read AU repository state journal files if needed
    if (!artifactStates.containsKey(aid.getId())) {
      for (Path journalPath :
          getAuJournalPaths(aid.getCollection(), aid.getAuid(), ArtifactRepositoryState.LOCKSS_JOURNAL_ID)) {

        // Get journal entries from file
        List<ArtifactRepositoryState> journal = readAuJournalEntries(journalPath, ArtifactRepositoryState.class);

        for (ArtifactRepositoryState journalEntry : journal) {

          // Get current state from map
          ArtifactRepositoryState state = artifactStates.get(journalEntry.getArtifactId());

          // Update map if entry not found or if the journal entry (for an artifact) is equal to or newer
          // FIXME Any finite resolution implementation of Instant is going to be problematic here, given a sufficiently
          //       fast machine. The effect of equals() here is, falling back to the order in which the journal
          //       entries appear (appended) in a journal file and the order in which journal files are read.
          if (state == null ||
              journalEntry.getEntryDate().equals(state.getEntryDate()) ||
              journalEntry.getEntryDate().isAfter(state.getEntryDate())) {

            artifactStates.put(journalEntry.getArtifactId(), journalEntry);
          }
        }
      }
    }

    return artifactStates.get(aid.getId());
  }

  /**
   * Reads the journal for a class of artifact metadata from a WARC file at a given path, and builds a map from artifact
   * ID to its most recently journaled metadata.
   *
   * @param journalPath A {@code String} containing the path to the WARC file containing artifact metadata.
   * @return A {@code Map<String, JSONObject>} mapping artifact ID to its latest metadata.
   * @throws IOException
   */
  protected synchronized <T> List<T> readAuJournalEntries(Path journalPath, Class<T> journalEntryClass) throws IOException {
    List<T> journalEntries = new ArrayList<>();

    log.trace("journalPath = {}", journalPath);

    try (InputStream warcStream = markAndGetInputStream(journalPath)) {
      // FIXME: Move this to constructor
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      for (ArchiveRecord record : new UncompressedWARCReader(getClass().getSimpleName(), warcStream)) {
        // Determine WARC record type
        WARCRecordType warcRecordType =
            WARCRecordType.valueOf((String) record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE));

        switch (warcRecordType) {
          case metadata:
            T journalEntry = mapper.readValue(record, journalEntryClass);
            journalEntries.add(journalEntry);

            break;

          case warcinfo:
            // TODO Do something useful?
            break;

          default:
            log.debug2("Skipped unexpected WARC record [WARC-Type: {}]", warcRecordType);
        }
      }

      return journalEntries;
    }
  }

  /**
   * Reads and replays repository metadata to a given artifact index.
   *
   * @param index        An {@code ArtifactIndex} to replay repository metadata to.
   * @param journalPath A {@code String} containing the path to a repository metadata journal WARC file.
   * @throws IOException
   */
  protected void replayArtifactRepositoryStateJournal(ArtifactIndex index, Path journalPath) throws IOException {
    for (ArtifactRepositoryState state : readAuJournalEntries(journalPath, ArtifactRepositoryState.class)) {
      // Get the artifact ID of this repository metadata
      String artifactId = state.getArtifactId();

      log.trace("artifactState = {}", state.toJson());

      log.debug("Replaying repository metadata for artifact {} from repository metadata file {}",
          artifactId,
          journalPath
      );

      // Replay to artifact index
      if (index.artifactExists(artifactId)) {
        if (state.isDeleted()) {
          log.debug2("Removing artifact {} from index", artifactId);
          index.deleteArtifact(artifactId);
          continue;
        }

        if (state.isCommitted()) {
          log.debug2("Marking artifact {} as committed in index", artifactId);
          index.commitArtifact(artifactId);
        }
      } else {
        if (!state.isDeleted()) {
          log.warn("Artifact referenced by journal is not deleted but doesn't exist in index! [artifactId: {}]", artifactId);
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

    // Use fetch time property from artifact for WARC-Date if present
    String fetchTimeValue = artifactData.getMetadata().getFirst(Constants.X_LOCKSS_FETCH_TIME);

    long fetchTime = -1;

    if (fetchTimeValue != null) {
      try {
        fetchTime = Long.valueOf(fetchTimeValue);
      } catch (NumberFormatException e) {
        // Ignore
      }
    }

    // Fallback to collection date from artifact if fetch time property is missing
    if (fetchTime < 0) {
      fetchTime = artifactData.getCollectionDate();
    }

    // Set WARC-Date field; default to now() if fetch time property and collection date not present
    record.setCreate14DigitDate(
        DateTimeFormatter.ISO_INSTANT.format(
            fetchTime > 0 ?
                Instant.ofEpochMilli(fetchTime).atZone(ZoneOffset.UTC) :
                Instant.now().atZone(ZoneOffset.UTC)
        ));

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

    record.addExtraHeader(
        ArtifactConstants.ARTIFACT_STORED_DATE,
        DateTimeFormatter.ISO_INSTANT.format(
            // Inherit stored date if set (e.g., in the temporary WARC record)
            artifactData.getStoredDate() > 0 ?
                Instant.ofEpochMilli(artifactData.getStoredDate()).atZone(ZoneOffset.UTC) :
                Instant.now().atZone(ZoneOffset.UTC)
        )
    );

    // We're required to pre-compute the WARC payload size (which is an artifact encoded as an HTTP response stream)
    // but it is not possible to determine the final size without exhausting the InputStream, so we use a
    // DeferredFileOutputStream, copy the InputStream into it, and determine the number of bytes written.
    DeferredTempFileOutputStream dfos = new DeferredTempFileOutputStream((int) DEFAULT_DFOS_THRESHOLD, "writeArtifactData");
    try {
      artifactData.setComputeDigestOnRead(true);
      // Create a HTTP response stream from the ArtifactData
      InputStream httpResponse = ArtifactDataUtil.getHttpResponseStreamFromHttpResponse(
          ArtifactDataUtil.getHttpResponseFromArtifactData(artifactData)
      );

      IOUtils.copyLarge(httpResponse, dfos);

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
   * @param state A RepositoryArtifactMetadata with the artifact metadata.
   * @return A WARCRecordInfo representing the given artifact metadata.
   */
  // FIXME Make this generic
  public static WARCRecordInfo createWarcMetadataRecord(String refersTo, ArtifactRepositoryState state) {
    // Create a WARC record object
    WARCRecordInfo record = new WARCRecordInfo();

    // Set record content stream
    byte[] jsonBytes = state.toJson().toString().getBytes();
    record.setContentStream(new ByteArrayInputStream(jsonBytes));

    // Mandatory WARC record headers
    record.setRecordId(URI.create(UUID.randomUUID().toString()));
    record.setCreate14DigitDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));
    record.setType(WARCRecordType.metadata);
    record.setContentLength(jsonBytes.length);
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
    // Write the WARC record header
    writeWarcRecordHeader(record, out);

    if (record.getContentStream() != null) {
      // Write the WARC record payload
      long bytesWritten = IOUtils.copyLarge(record.getContentStream(), out);

      // Sanity check
      if (bytesWritten != record.getContentLength()) {
        log.warn(
            "Number of bytes written did not match Content-Length header (expected: {} bytes, wrote: {} bytes)",
            record.getContentLength(),
            bytesWritten
        );
      }
    }

    // Write the two CRLF blocks required at end of every record (per the spec)
    out.write(CRLF_BYTES);
    out.write(CRLF_BYTES);
  }

  public static void writeWarcRecordHeader(WARCRecordInfo record, OutputStream out) throws IOException {
    // Write the header
    out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

    // Write a CRLF block to separate header from body
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
