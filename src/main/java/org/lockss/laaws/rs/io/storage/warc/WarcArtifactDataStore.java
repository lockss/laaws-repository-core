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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.http.HttpException;
import org.apache.solr.common.SolrException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.util.anvl.Element;
import org.archive.util.zip.GZIPMembersInputStream;
import org.lockss.laaws.rs.core.BaseLockssRepository;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.SemaphoreMap;
import org.lockss.laaws.rs.impl.ArtifactContainerStats;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.CollectionAuidPair;
import org.lockss.laaws.rs.util.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.CloseCallbackInputStream;
import org.lockss.util.Constants;
import org.lockss.util.LockssUncheckedIOException;
import org.lockss.util.concurrent.stripedexecutor.StripedCallable;
import org.lockss.util.concurrent.stripedexecutor.StripedExecutorService;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.io.FileUtil;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeUtil;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This abstract class aims to capture operations that are common to all {@link ArtifactDataStore} implementations that
 * serialize {@link ArtifactData} as WARC records in a WARC file.
 */
// TODO Get rid of generics
public abstract class WarcArtifactDataStore implements ArtifactDataStore<ArtifactIdentifier, ArtifactData, ArtifactStateEntry>, WARCConstants {

  private final static L4JLogger log = L4JLogger.getLogger();

  // DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS") does not parse in Java 8: https://bugs.openjdk.java.net/browse/JDK-8031085
  protected static final DateTimeFormatter FMT_TIMESTAMP =
      new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss")
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withZone(ZoneId.of("UTC"));

  protected static final String AU_DIR_PREFIX = "au-";

  protected static final String COLLECTIONS_DIR = "collections";
  protected static final String TMP_WARCS_DIR = "tempwarcs";

  protected static final String WARCID_SCHEME = "urn:uuid";
  protected static final String CRLF = "\r\n";
  protected static byte[] CRLF_BYTES = CRLF.getBytes(StandardCharsets.US_ASCII);
  protected static byte[] TWO_CRLF_BYTES = (CRLF + CRLF).getBytes(StandardCharsets.US_ASCII);

  public static final Path DEFAULT_BASEPATH = Paths.get("/lockss");
  public final static String DEFAULT_TMPWARCBASEPATH = TMP_WARCS_DIR;

  private static final long DEFAULT_DFOS_THRESHOLD = 16L * FileUtils.ONE_MB;

  protected final static long MAX_AUACTIVEWARCS_RELOADED = 10;

  protected static final String ENV_THRESHOLD_WARC_SIZE = "REPO_MAX_WARC_SIZE";
  protected static final long DEFAULT_THRESHOLD_WARC_SIZE = 1L * FileUtils.ONE_GB;
  protected long thresholdWarcSize;

  protected static final String ENV_THRESHOLD_ARTIFACTS = "REPO_MAX_ARTIFACTS";
  public static final int DEFAULT_THRESHOLD_ARTIFACTS = 1000;
  protected int thresholdArtifacts;

  protected static final String ENV_UNCOMMITTED_ARTIFACT_EXPIRATION = "REPO_UNCOMMITTED_ARTIFACT_EXPIRATION";
  protected static final long DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION = 4 * TimeUtil.HOUR;
  protected long uncommittedArtifactExpiration;

  protected static final String ENV_TMPWARCGC_INTERVAL = "REPO_TMPWARCGC_INTERVAL";
  protected static final long DEFAULT_TMPWARCGC_INTERVAL = 10 * TimeUtil.MINUTE;
  protected long tmpWarcGCInterval;

  protected Path[] basePaths;
  protected WarcFilePool tmpWarcPool;
  protected Map<CollectionAuidPair, List<Path>> auActiveWarcsMap = new HashMap<>();
  protected Map<CollectionAuidPair, List<Path>> auPathsMap = new HashMap<>();

  private Map<ArtifactIdentifier, CopyArtifactTask> queuedCopyTasks = new ConcurrentHashMap<>();

  private BaseLockssRepository repo;

  protected DataStoreState dataStoreState = DataStoreState.STOPPED;

  protected boolean useCompression;

  protected FutureRecordingStripedExecutorService stripedExecutor;


  public enum DataStoreState {
    INITIALIZED,
    RUNNING,
    STOPPED
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
  public WarcArtifactDataStore() {
    // Create striped executor service
    this.stripedExecutor = new FutureRecordingStripedExecutorService();

    // Set WARC threshold size to use
    setThresholdWarcSize(NumberUtils.toLong(System.getenv(ENV_THRESHOLD_WARC_SIZE), DEFAULT_THRESHOLD_WARC_SIZE));

    // Set temporary WARC GC interval
    setTmpWarcGCInterval(NumberUtils.toLong(System.getenv(ENV_TMPWARCGC_INTERVAL), DEFAULT_TMPWARCGC_INTERVAL));

    // Set WARC artifacts threshold to use
    setThresholdArtifacts(NumberUtils.toInt(System.getenv(ENV_THRESHOLD_ARTIFACTS), DEFAULT_THRESHOLD_ARTIFACTS));

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
  public void init() {
    log.debug("Initializing data store");
    setDataStoreState(DataStoreState.INITIALIZED);
  }

  @Override
  public void start() {
    log.debug("Starting data store");
    reloadDataStoreState();
    scheduleGarbageCollector();
    setDataStoreState(DataStoreState.RUNNING);
  }

  /**
   * Shutdowns the data store.
   *
   * @throws InterruptedException
   */
  @Override
  public void stop() {
    if (dataStoreState != DataStoreState.STOPPED) {
      stripedExecutor.shutdown();

      try {
        // TODO: Parameterize
        stripedExecutor.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        log.warn("Executor interrupted while awaiting termination", e);
      }

      setDataStoreState(DataStoreState.STOPPED);

      log.info("Finished shutdown of data store");
    } else {
      log.info("Data store is already stopped");
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

  protected void reloadDataStoreState() {
    stripedExecutor.submit(new ReloadDataStoreStateTask());
  }

  /**
   * Asynchronous data store reload tasks for non-volatile storage implementations.
   */
  public class ReloadDataStoreStateTask implements Runnable {
    @Override
    public void run() {
      try {
        //// Reload temporary WARCs
        for (Path tmpBasePath : getTmpWarcBasePaths()) {
          reloadTemporaryWarcs(getArtifactIndex(), tmpBasePath);
        }

        //// TODO: Reload active WARCs
        // reloadActiveWarcs();
      } catch (Exception e) {
        log.error("Could not complete asynchronous data store reload", e);
        throw new IllegalStateException("Could not complete asynchronous reload", e);
      }
    }
  }

  /**
   * Wait for all background commits for an AU to finish
   */
  public boolean waitForCommitTasks(String collection, String auid) {
    log.debug2("Waiting for stripe " + new CollectionAuidPair(collection, auid));

    return stripedExecutor.waitForStripeToEmpty(new CollectionAuidPair(collection, auid));
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
  protected Path getBasePathFromStorageUrl(URI storageUrl) {
    Path warcPath = Paths.get(storageUrl.getPath());

    return Arrays.stream(getBasePaths())
        .filter(basePath -> warcPath.startsWith(basePath.toString()))
        .sorted(Comparator.reverseOrder()) // Q: Is this right?
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Storage URL has no common base path"));
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
   * @param collectionId   A {@link String} containing the name of the collection the AU belongs to.
   * @param auid           A {@link String} containing the AUID of the AU.
   * @param minSize        A {@code long} containing the minimum available space the underlying base path must have in bytes.
   * @param compressedWarc A {@code boolean} indicating a compressed active WARC is needed.
   * @return A {@link Path} containing the path of the chosen active WARC.
   * @throws IOException
   */
  public Path getAuActiveWarcPath(String collectionId, String auid, long minSize, boolean compressedWarc) throws IOException {
    synchronized (auActiveWarcsMap) {
      // Get all the active WARCs of this AU
      List<Path> activeWarcs = getAuActiveWarcPaths(collectionId, auid);

      // Filter active WARCs by compression
      List<Path> fActiveWarcs = activeWarcs.stream()
          .filter(p -> isCompressedWarcFile(p) == compressedWarc)
          .collect(Collectors.toList());

      // If there are multiple active WARCs for this AU, pick the one under the base path with the most free space
      Path activeWarc = getMinMaxFreeSpacePath(fActiveWarcs, minSize);

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
   * Internal convenience method for use in Streams.
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
   * Internal convenience method for use in Streams.
   * <p>
   * Returns length of WARC file in bytes or zero if the WARC file doesn't exist or there was some
   * other issue accessing its length.
   */
  private long getWarcLengthOrZero(Path path) {
    try {
      return getWarcLength(path);
    } catch (IOException e) {
      log.warn("Caught IOException", e);
      return 0L;
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

  /**
   * Returns the paths to WARC files containing artifacts in an AU.
   *
   * @param collectionId A {@link String} containing the collection ID of the AU.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link List<Path>} containing the paths to the WARC files.
   * @throws IOException
   */
  protected List<Path> findAuArtifactWarcs(String collectionId, String auid) throws IOException {
    return findAuArtifactWarcsStream(collectionId, auid).collect(Collectors.toList());
  }

  /**
   * Returns the paths to WARC files containing artifacts in an AU.
   *
   * @param collectionId A {@link String} containing the collection ID of the AU.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link List<Path>} containing the paths to the WARC files.
   * @throws IOException
   */
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
    return getAuPath(basePath, collection, auid).resolve(journalName + WARCConstants.DOT_WARC_FILE_EXTENSION);
  }

  /**
   * Returns the preferred WARC file extension based on whether compression is in use.
   *
   * @return A {@link String} containing the preferred file extension.
   */
  protected String getWarcFileExtension() {
    return useCompression ?
        WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION :
        WARCConstants.DOT_WARC_FILE_EXTENSION;
  }

  /**
   * Returns true if the file name ends with the compressed WARC file extension (.warc.gz).
   *
   * @param warcFile A {@link Path} containing the path to a WARC file.
   * @return A {@code boolean} indicating whether the {@link Path} points to a compressed WARC file.
   */
  public boolean isCompressedWarcFile(Path warcFile) {
    return warcFile.getFileName().toString()
        .endsWith(WARCReaderFactory.DOT_COMPRESSED_WARC_FILE_EXTENSION);
  }

  /**
   * Returns the path to a journal of an AU across all the configured data store base paths.
   *
   * @param journalName A {@link String} containing the name of the journal.
   * @return A {@link Path[]} containing the paths to the journal on across all the configured data store base paths.
   */
  protected Path[] getAuJournalPaths(String collection, String auid, String journalName) throws IOException {
    return getAuPaths(collection, auid).stream()
        .map(auPath -> auPath.resolve(journalName + WARCConstants.DOT_WARC_FILE_EXTENSION))
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
  // FIXME: Processes calling this is probably aren't utilizing storage URLs correctly
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
    return String.format("artifacts_%s-%s_%s", collectionId, auidHash, timestamp);
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
    Path auActiveWarc = auPath.resolve(generateActiveWarcName(collectionId, auid) + getWarcFileExtension());

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

  protected void scheduleGarbageCollector() {
    log.debug("Scheduling temporary WARC garbage collection");

    repo.getScheduledExecutorService().scheduleWithFixedDelay(
        new GCTemporaryWarcsTask(tmpWarcPool), tmpWarcGCInterval, tmpWarcGCInterval, TimeUnit.MILLISECONDS);
  }

  private class GCTemporaryWarcsTask implements Runnable {
    private final WarcFilePool pool;

    public GCTemporaryWarcsTask(WarcFilePool pool) {
      this.pool = pool;
    }

    @Override
    public void run() {
      pool.runGC();
    }
  }

  /**
   * Reads and reloads state from temporary WARCs, including the requeuing of copy tasks of committed artifacts from
   * temporary to permanent storage. Removes temporary WARCs if eligible:
   * <p>
   * A temporary WARC may be removed if all the records contained within it are the serializations of artifacts that are
   * either uncommitted-but-expired, committed-and-moved-to-permanent-storage, or deleted.
   */
  public void reloadTemporaryWarcs(ArtifactIndex index, Path tmpWarcBasePath) throws IOException {
    if (index == null) {
      throw new IllegalArgumentException("Null artifact index");
    }

    log.info("Reloading temporary WARCs from {}", tmpWarcBasePath);

    Collection<Path> tmpWarcs = findWarcs(tmpWarcBasePath);

    log.debug("Found {} temporary WARCs: {}", tmpWarcs.size(), tmpWarcs);

    // Iterate over the temporary WARC files that were found
    for (Path tmpWarc : tmpWarcs) {
      if (!tmpWarcPool.isInPool(tmpWarc)) {
        reloadOrRemoveTemporaryWarc(index, tmpWarc);
      }
    }

    log.debug("Finished reloading temporary WARCs from {}", tmpWarcBasePath);
  }

  /**
   * Used to workaround an issue in webarchive-commons: Prevents a double close() on a
   * compressed WARC input stream.
   */
  private static class IgnoreCloseInputStream extends FilterInputStream {
    public IgnoreCloseInputStream(InputStream stream) {
      super(stream);
    }

    public void close() throws IOException {
    }
  }

  /**
   * Reloads artifacts from a temporary WARC file and resumes their lifecycle in this WARC artifact data store. If
   * the artifacts in this data store are no longer needed, the temporary WARC file is deleted.
   *
   * @param index The {@link ArtifactIndex} used to determine artifact state.
   * @param tmpWarc A {@link Path} to the temporary WARC file to examine.
   * @throws IOException Thrown if there are any I/O errors.
   */
  protected void reloadOrRemoveTemporaryWarc(ArtifactIndex index, Path tmpWarc) throws IOException {
    log.trace("tmpWarc = {}", tmpWarc);

    boolean isWarcFileRemovable = true;

    // Open WARC file
    try (InputStream warcStream = markAndGetInputStream(tmpWarc)) {

      ArchiveReader archiveReader =
          getArchiveReader(tmpWarc, new IgnoreCloseInputStream(warcStream));

      // Do not perform digest calculations
      archiveReader.setDigest(false);

      // ArchiveReader is an iterable over ArchiveRecord objects
      for (ArchiveRecord record : archiveReader) {
        boolean isRecordRemovable = false;

        ArtifactIdentifier aid = ArtifactDataFactory.buildArtifactIdentifier(record.getHeader());

        // Acquire artifact lock: Operations below alter artifact state
        lockArtifact(aid);

        Artifact artifact = getArtifactIndex().getArtifact(aid);
        ArtifactState state = getArtifactState(artifact, isArtifactExpired(record));

        // Resume artifact lifecycle based on the artifact's state
        try {
          switch (state) {
            case UNCOMMITTED:
              break;

            case PENDING_COPY:
              // Requeue the copy of this artifact from temporary to permanent storage
              CopyArtifactTask task = new CopyArtifactTask(artifact);
              queuedCopyTasks.put(artifact.getIdentifier(), task);
              stripedExecutor.submit(task);
              break;

            case EXPIRED:
              // Remove artifact reference from index if it exists
              if (!index.deleteArtifact(aid.getId())) {
                log.warn("Could not remove expired artifact from index [artifactId: {}]", aid.getId());
              }

            case UNKNOWN:
            case NOT_INDEXED:
            case COPIED:
            case DELETED:
              log.debug2("WARC record is removable [state: {}, warcId: {}, tmpWarc: {}]",
                  state, record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID), tmpWarc);

              // Mark this temporary WARC record as removable
              isRecordRemovable = true;
              break;

            default:
              log.warn("Unknown artifact state [artifactId: {}, state: {}]", artifact.getId(), state);
              break;
          }
        } finally {
          releaseArtifactLock(aid);
        }

        // All records must be removable for temporary WARC file to be removable
        isWarcFileRemovable &= isRecordRemovable;
      }
    } catch (IOException e) {
      log.error("Could not reload temporary WARC [tmpWarc: {}]", tmpWarc);
      throw e;
    }

    // Protect against a reader still/already reading from temporary WARC even if WARC is now removable
    boolean isInUse = TempWarcInUseTracker.INSTANCE.isInUse(tmpWarc);

    log.debug2("tmpWarc: {}, isWarcFileRemovable: {}, isInUse: {}",
        tmpWarc, isWarcFileRemovable, isInUse);

    // Remove file depending on results
    if (isWarcFileRemovable && !isInUse) {
      try {
        log.debug("Removing temporary WARC file [tmpWarc: {}]", tmpWarc);
        removeWarc(tmpWarc);
      } catch (IOException e) {
        log.warn("Could not remove a removable temporary WARC file", e);
        // Try again later - avoid reprocessing by marking as already processed and removable?
      }
    }
  }

  /**
   * Determines whether a temporary WARC file is removable.
   * <p>
   * A temporary WARC file is removable if all the WARC records contained within it may be removed.
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
      ArchiveReader archiveReader = getArchiveReader(tmpWarc, warcStream);
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
  protected boolean isTempWarcRecordRemovable(ArchiveRecord record) throws IOException {
    // Get WARC record headers
    ArchiveRecordHeader headers = record.getHeader();

    // Get artifact identifier from WARC header
    ArtifactIdentifier aid = ArtifactDataFactory.buildArtifactIdentifier(headers);

    // Get the WARC type
    String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

    switch (WARCRecordType.valueOf(recordType)) {
      case response:
      case resource:
        // Lock artifact
        lockArtifact(aid);

        try {
          Artifact indexed = getArtifactIndex().getArtifact(aid);
          ArtifactState state = getArtifactState(indexed, isArtifactExpired(record));

          switch (state) {
            case NOT_INDEXED:
            case COPIED:
            case EXPIRED:
            case DELETED:
              return true;

            case UNKNOWN:
              log.warn("Unknown artifact state [artifact: {}, state: {}]", indexed, state);
            case UNCOMMITTED:
            case PENDING_COPY:
            default:
              return false;
          }
        } finally {
          releaseArtifactLock(aid);
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
   * Semaphore map keyed by {@link ArtifactIdentifier}. Used to synchronize operations on an artifact.
   */
  private SemaphoreMap<ArtifactIdentifier> artifactLock = new SemaphoreMap<>();

  /**
   * Returns the {@link ArtifactState} of an artifact in this data store. Not thread-safe!
   */
  protected ArtifactState getArtifactState(Artifact artifact, boolean isExpired) throws IOException {
    // ********************************
    // Determine if artifact is deleted
    // ********************************
    if (artifact == null) {
      return ArtifactState.DELETED;
    }

    // ************************
    // Determine artifact state
    // ************************

    try {
      if (artifact.isCommitted() && !isTmpStorage(getPathFromStorageUrl(new URI(artifact.getStorageUrl())))) {
        // Artifact is marked committed and in permanent storage
        return ArtifactState.COPIED;
      } else if (artifact.isCommitted()) {
        // Artifact is marked committed but not copied to permanent storage
        return ArtifactState.PENDING_COPY;
      } else if (!artifact.isCommitted() && !isExpired) {
        // Uncommitted and not copied but indexed
        return ArtifactState.UNCOMMITTED;
      } else if (isExpired) {
        return ArtifactState.EXPIRED;
      }
    } catch (URISyntaxException e) {
      // This should never happen; storage URLs are generated internally
      log.error("Bad storage URL: [artifact: {}]", artifact, e);
      return ArtifactState.UNKNOWN;
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
    return Instant.ofEpochMilli(TimeBase.nowMs()).isAfter(expiration);
  }

  /**
   * Returns a boolean indicating whether an artifact is marked as deleted in the journal.
   *
   * @param aid The {@link ArtifactIdentifier} of the artifact to check.
   * @return A {@code boolean} indicating whether the artifact is marked as deleted.
   * @throws IOException
   */
  protected boolean isArtifactDeleted(ArtifactIdentifier aid) throws IOException {
    // Check whether the artifact indexed
    return !getArtifactIndex().artifactExists(aid.getId());
  }

  /**
   * Returns a boolean indicating whether an artifact is marked as committed in the journal.
   *
   * @param aid The {@link ArtifactIdentifier} of the artifact to check.
   * @return A {@code boolean} indicating whether the artifact is marked as committed.
   * @throws IOException
   */
  protected boolean isArtifactCommitted(ArtifactIdentifier aid) throws IOException {
    synchronized (getArtifactIndex()) {
      Artifact artifact = getArtifactIndex().getArtifact(aid);

      if (artifact != null) {
        return artifact.isCommitted();
      }
    }

    throw new LockssNoSuchArtifactIdException();
  }

  // *******************************************************************************************************************
  // * GETTERS AND SETTERS
  // *******************************************************************************************************************

  /**
   * Returns a {@code boolean} indicating whether this WARC artifact data store compresses WARC
   * records.
   */
  public boolean getUseWarcCompression() {
    return useCompression;
  }

  /**
   * Sets whether this WARC data store compresses WARCs files.
   *
   * @param useCompression A {@code boolean} indicating whether to compress WARC files.
   */
  public void setUseWarcCompression(boolean useCompression) {
    log.trace("useCompression = {}", useCompression);
    this.useCompression = useCompression;
  }

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
   * @param ms A {@code long}
   */
  public void setUncommittedArtifactExpiration(long ms) {
    this.uncommittedArtifactExpiration = ms;
  }

  /**
   * Sets the delay (in number of milliseconds) between temporary WARC GCs.
   */
  public void setTmpWarcGCInterval(long ms) {
    this.tmpWarcGCInterval = ms;
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
   * Returns the maximum number of artifacts that should be added to a temporary WARC file before
   * it is closed from further writes.
   */
  public int getMaxArtifactsThreshold() {
    return thresholdArtifacts;
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

  public void setThresholdArtifacts(int artifacts) {
    if (artifacts < 0) {
      throw new IllegalArgumentException("Threshold number of artifacts must be a positive integer");
    }

    thresholdArtifacts = artifacts;
  }

  public void setLockssRepository(BaseLockssRepository repository) {
    this.repo = repository;
  }

  /**
   * Configures the artifact index associated with this WARC artifact data store.
   * <p>
   * Should only be used in testing.
   * <p>
   * Deprecated. Use internal handle to {@link BaseLockssRepository} instead.
   *
   * @param artifactIndex The {@code ArtifactIndex} instance to associate with this WARC artifact data store.
   */
  @Deprecated
  protected void setArtifactIndex(ArtifactIndex artifactIndex) {
    repo.setArtifactIndex(artifactIndex);
  }

  /**
   * Return the artifact index associated with this WARC artifact data store.
   *
   * @return The {@code ArtifactIndex} associated with this WARC artifact data store.
   */
  public ArtifactIndex getArtifactIndex() {
    return repo.getArtifactIndex();
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

    log.debug2("Adding artifact [artifactId: {}]", artifactId);

    try {
      // ********************************
      // Write artifact to temporary WARC
      // ********************************

      // Get a temporary WARC from the temporary WARC pool
      WarcFile tmpWarc = tmpWarcPool.checkoutWarcFileForWrite();
      Path tmpWarcPath = tmpWarc.getPath();

      // Record will be appended to the WARC file; its offset is the current length of the WARC
      long offset = getWarcLength(tmpWarcPath);
      long recordLength = 0;
      long storedRecordLength = 0;

      // Write serialized artifact to temporary WARC file
      try (OutputStream output = getAppendableOutputStream(tmpWarcPath)) {

        // Use a CountingOutputStream to track number of bytes written to the WARC (i.e., size
        // of the WARC record compressed or uncompressed)
        try (CountingOutputStream cos = new CountingOutputStream(output)) {
          if (useCompression) {
            // Yes - wrap COS in GZIPOutputStream then write to it
            try (GZIPOutputStream gzipOutput = new GZIPOutputStream(cos)) {
              recordLength = writeArtifactData(artifactData, gzipOutput);
            }
          } else {
            // No - write to COS directly
            recordLength = writeArtifactData(artifactData, cos);
          }

          storedRecordLength = cos.getCount();
        }

        // Update WARC file stats
        synchronized (tmpWarc) {
          tmpWarc.incrementLength(storedRecordLength);
          ArtifactContainerStats tmpWarcStats = tmpWarc.getStats();
          tmpWarcStats.incArtifactsTotal();
          tmpWarcStats.incArtifactsUncommitted();
          tmpWarcStats.setLatestExpiration(TimeBase.nowMs() + getUncommittedArtifactExpiration());
        }

        // Debugging
        log.debug2("Wrote {} bytes offset {} to {}; size is now {}",
            storedRecordLength, offset, tmpWarcPath, offset + recordLength);

        if (useCompression) {
          log.debug2("WARC record compression ratio: {} [compressed: {}, uncompressed: {}]",
              (float) recordLength / storedRecordLength, storedRecordLength, recordLength);
        }
      } catch (IOException e) {
        // Error writing artifact to WARC: Close WARC from further writes
        log.error("Could not write artifact to temporary WARC", e);
        tmpWarc.release();
        throw e;
      } finally {
        // Return temporary WARC file to pool
        tmpWarcPool.returnWarcFile(tmpWarc);
      }

      // Update ArtifactData object with new properties
      artifactData.setStorageUrl(makeWarcRecordStorageUrl(tmpWarcPath, offset, storedRecordLength));

      // ******************
      // Index the artifact
      // ******************

      getArtifactIndex().indexArtifact(artifactData);

      // *******************************
      // Write artifact state to journal
      // *******************************

      // Write journal entry to journal file under an existing AU path
      List<Path> auPaths = getAuPaths(artifactId.getCollection(), artifactId.getAuid());

      Path auPath = auPaths.stream()
          .sorted((a, b) -> (int) (getFreeSpace(b) - getFreeSpace(a)))
          .findFirst()
          .orElse(null); // should never happen

      Path auBasePath = Arrays.stream(getBasePaths())
          .sorted()
          .filter(bp -> auPath.startsWith(bp))
          .findFirst()
          .orElse(null); // should never happen

      // Write journal entry
      updateArtifactStateJournal(auBasePath, artifactId,
          new ArtifactStateEntry(artifactId, ArtifactState.UNCOMMITTED));

      // *******************
      // Return the artifact
      // *******************

      // Create a new Artifact object to return
      Artifact artifact = new Artifact(
          artifactId,
          false,
          artifactData.getStorageUrl().toString(),
          artifactData.getContentLength(),
          artifactData.getContentDigest());

      // Set the artifact collection date
      artifact.setCollectionDate(artifactData.getCollectionDate());

      log.debug("Added artifact {}", artifact);

      return artifact;

    } catch (Exception e) {
      log.error("Could not add artifact data", e);
      throw e;
    }
  }

  /**
   * Retrieves the {@link ArtifactData} of an {@link Artifact} by resolving its storage URL.
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

    String artifactId = artifact.getId();
    Artifact indexedArtifact;
    URI storageUrl;

    Path warcFilePath = null;
    boolean isTmpStorage = false;
    InputStream warcStream = null;

    try {
      // This could interact with two other processes:
      // 1. The GC process could remove the temporary WARC file from under this method
      // 2. The copy process could change the storage URL to point to permanent storage

      // Retrieve artifact reference from index
      indexedArtifact = getArtifactIndex().getArtifact(artifactId);

      if (indexedArtifact == null) {
        // Yes: Artifact reference not found in index
        log.debug("Artifact not found in index [artifactId: {}]", artifactId);
        throw new LockssNoSuchArtifactIdException("Artifact not found");
      }

      lockArtifact(indexedArtifact.getIdentifier());

      try {
        // Get storage URL and WARC path of artifact's WARC record
        storageUrl = new URI(indexedArtifact.getStorageUrl());
        warcFilePath = getPathFromStorageUrl(storageUrl);
        isTmpStorage = isTmpStorage(warcFilePath);

        if (isTmpStorage) {
          String expiredErrorMsg = "Artifact expired and was GCed";

          WarcFile warcFile = tmpWarcPool.getWarcFile(warcFilePath);

          // If the WARC file is now gone, it means that the temp WARC GC decided it could be delete
          // in which case this artifact must be expired:
          if (warcFile == null) {
            log.error(expiredErrorMsg);
            throw new LockssNoSuchArtifactIdException(expiredErrorMsg);
          }

          synchronized (warcFile) {
            if (warcFile.isMarkedForGC()) {
              log.error(expiredErrorMsg);
              throw new LockssNoSuchArtifactIdException(expiredErrorMsg);
            } else {
              // Increment usage counter of temporary WARC -- cannot now mark for GC
              TempWarcInUseTracker.INSTANCE.markUseStart(warcFilePath);
            }
          }
        }
      } catch (URISyntaxException e) {
        // This should never happen since storage URLs are internal
        log.error("Malformed storage URL [storageUrl:  {}]", indexedArtifact.getStorageUrl());
        throw new IllegalArgumentException("Malformed storage URL");
      } finally {
        releaseArtifactLock(indexedArtifact.getIdentifier());
      }

      log.debug2("artifactId: {}, storageUrl: {}", artifactId, storageUrl);

      // Open an InputStream from the WARC file and get the WARC record representing this artifact data
      warcStream = getInputStreamFromStorageUrl(storageUrl);

      // Wrap uncompressed stream in GZIPInputStream if the file is compressed
      if (isCompressedWarcFile(warcFilePath)) {
        GZIPInputStream gzipInputStream = new GZIPInputStream(warcStream);
        warcStream = new SimpleRepositionableStream(gzipInputStream);
      }

      if (isTmpStorage) {
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
      ArtifactIdentifier indexedArtifactId = indexedArtifact.getIdentifier();
      artifactData.setIdentifier(indexedArtifactId);
      artifactData.setStorageUrl(URI.create(indexedArtifact.getStorageUrl()));
      artifactData.setContentLength(indexedArtifact.getContentLength());
      artifactData.setContentDigest(indexedArtifact.getContentDigest());

      // Set artifact's state
      // FIXME: This could probably be cleaned up...
      if (!isTmpStorage) {
        artifactData.setArtifactState(ArtifactState.COPIED);
      } else if (isArtifactCommitted(indexedArtifactId)) {
        artifactData.setArtifactState(ArtifactState.PENDING_COPY);
      } else {
        // Q: Do we need to distinguish UNCOMMITTED from EXPIRED here? DELETED and NOT_INDEXED
        //  don't make sense since we needed a non-null Artifact to get to this point.
        artifactData.setArtifactState(ArtifactState.UNCOMMITTED);
      }

      // Return an ArtifactData from the WARC record
      return artifactData;

    } catch (Exception e) {
      log.error("Could not get artifact data [artifactId: {}, storageUrl: {}]", artifact.getId(),
          artifact.getStorageUrl(), e);

      if (warcStream != null) {
        IOUtils.closeQuietly(warcStream);
      }

      if (isTmpStorage) {
        TempWarcInUseTracker.INSTANCE.markUseEnd(warcFilePath);
      }

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

    ArtifactIdentifier artifactId = artifact.getIdentifier();

    log.trace("artifact = {}", artifact);

    // Acquire artifact lock
    lockArtifact(artifactId);

    try {
      // Determine what action to take based on the state of the artifact. Hardwired isExpired
      // parameter to false. The effect is expired artifacts will still be committed.
      Artifact indexed = getArtifactIndex().getArtifact(artifactId);
      ArtifactState state = getArtifactState(indexed, false);

      switch (state) {
        case UNCOMMITTED:
          getArtifactIndex().commitArtifact(artifact.getId());

          // Update temporary WARC file stats
          Path tmpWarcPath = getPathFromStorageUrl(new URI(indexed.getStorageUrl()));
          WarcFile tmpWarcFile = tmpWarcPool.getWarcFile(tmpWarcPath);

          if (tmpWarcFile == null) {
            log.error(
                "Too late to commit - artifact's temporary WARC has already been deleted [artifactId: {}, warc: {}]",
                artifact.getId(), tmpWarcPath);
            // TODO : Revisit whether to return null or throw
            return null;
          }

          synchronized (tmpWarcFile) {
            if (tmpWarcFile.isMarkedForGC()) {
              // Fail - too late to commit; we've already committed to GCing this WARC
              // TODO: Revisit whether to return null or throw
              return null;
            }

            ArtifactContainerStats tmpWarcStats = tmpWarcFile.getStats();
            tmpWarcStats.decArtifactsUncommitted();
            tmpWarcStats.incArtifactsCommitted();
          }

          // Mark artifact as committed in the journal
          ArtifactStateEntry artifactRepoState =
              new ArtifactStateEntry(artifact.getIdentifier(), ArtifactState.PENDING_COPY);

          // Write new state to journal
          updateArtifactStateJournal(
              getBasePathFromStorageUrl(new URI(artifact.getStorageUrl())),
              artifact.getIdentifier(),
              artifactRepoState);

          // Submit the task to copy the artifact data from temporary to permanent storage
          CopyArtifactTask task = new CopyArtifactTask(artifact);
          queuedCopyTasks.put(artifactId, task);
          return stripedExecutor.submit(task);

        case PENDING_COPY:
          // Duplicate commit call on this artifact - find and return the Future of the existing CopyArtifactTask
          CopyArtifactTask queuedTask = queuedCopyTasks.get(artifactId);

          // Could be null if CopyArtifactTask completed
          return queuedTask == null ?
              new CompletedFuture<>(getArtifactIndex().getArtifact(artifactId)) :
              queuedTask.getFuture();

        case COPIED:
          // This artifact is already in permanent storage. Wrap in Future and return it.
          return new CompletedFuture<>(indexed);

        case DELETED:
          log.warn("Cannot commit non-existent artifact [artifactId: {}, state: {}]",
              artifact.getId(), state.toString());

          // No Future to return
          return null;

        case NOT_INDEXED:
        case EXPIRED:
        case UNKNOWN:
        default:
          log.error("Unexpected artifact state; cannot commit [artifactId: {}, state: {}]", artifact.getId(),
              state.toString());

          // No Future to return
          return null;
      }
    } catch (URISyntaxException e) {
      // This should never happen since storage URLs are internal
      throw new IllegalStateException(e);
    } finally {
      releaseArtifactLock(artifactId);
    }
  }

  public void lockArtifact(ArtifactIdentifier artifactId) throws IOException {
    try {
      // Acquire the lock for this artifact
      artifactLock.getLock(artifactId);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while waiting to acquire artifact version lock");
    }
  }

  public void releaseArtifactLock(ArtifactIdentifier artifactId) {
    // Release the lock for the artifact
    artifactLock.releaseLock(artifactId);
  }

  public class FutureRecordingStripedExecutorService extends StripedExecutorService {
    @Override
    public Future<?> submit(Runnable task) {
      return super.submit(task);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      throw new UnsupportedOperationException("Not yet implemented");
    }

    public Future<Artifact> submit(CopyArtifactTask task) {
      RunnableFuture<Artifact> rf = newTaskFor(task);
      task.setFuture(rf);
      super.execute(rf);
      return rf;
    }
  }

  /**
   * Implementation of {@link Callable} that copies an artifact from temporary to permanent storage.
   * <p>
   * This is implemented as a {@link StripedCallable} because we maintain one active WARC file per AU.
   */
  protected class CopyArtifactTask implements StripedCallable<Artifact> {
    protected Artifact artifact;
    private boolean isDeleted = false;
    private Future<Artifact> future;

    public boolean isDeleted() {
      return isDeleted;
    }

    public void setDeleted() {
      isDeleted = true;
    }

    public Future<Artifact> getFuture() {
      return future;
    }

    public void setFuture(Future<Artifact> future) {
      this.future = future;
    }

    /**
     * Constructor of {@link CopyArtifactTask}.
     *
     * @param artifact The {@link Artifact} whose artifact data should be copied from temporary to permanent storage.
     */
    public CopyArtifactTask(Artifact artifact) {
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

    @Override
    public Artifact call() throws Exception {
      log.trace("Starting CopyArtifactTask: " + getStripe());

      try {
        return copyArtifact();
      } finally {
        // Remove task from queued copy map
        queuedCopyTasks.remove(artifact.getIdentifier());
      }
    }

    /**
     * Moves the WARC record of an artifact from temporary storage to a WARC in permanent storage, and updates the
     * storage URL if successful.
     *
     * @return
     * @throws Exception
     */
    private Artifact copyArtifact() throws IOException, URISyntaxException {
      // Artifact's storage URL
      URI storageUrl = new URI(artifact.getStorageUrl());
      Path storagePath = getPathFromStorageUrl(storageUrl);

      // Safeguard: Do not copy if already in permanent storage
      if (!isTmpStorage(storagePath)) {
        log.warn("Artifact is already copied [artifactId: {}]", artifact.getId());
        return artifact;
      }

      // Get the temporary WARC record location from the artifact's storage URL
      WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(new URI(artifact.getStorageUrl()));
      long recordOffset = loc.getOffset();
      long recordLength = loc.getLength();

      // Used to match source and target WARC compression
      boolean warcCompressionTarget = isCompressedWarcFile(loc.getPath());

      // Get an active WARC of this AU to append the artifact to
      Path dst = getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid(), recordLength, warcCompressionTarget);

      // Artifact will be appended as a WARC record to this WARC file so its offset is the current length of the file
      long warcLength = getWarcLength(dst);

      // *********************************
      // Append WARC record to active WARC
      // *********************************

      // 1. Mark temp WARC in use
      // 2. Open InputStream and copy artifact
      // 3. Update storage URL
      // 4. Unmark

      // Skip copy into permanent storage if artifact was deleted while this CopyArtifactTask was
      // sitting in the queue
      if (!isDeleted()) {
        // TODO: What to do about IOExceptions thrown here?
        try (OutputStream output = getAppendableOutputStream(dst)) {
          try (InputStream is = markAndGetInputStreamAndSeek(loc.getPath(), loc.getOffset())) {

            // *************
            // Copy artifact
            // *************

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

        // ******************
        // Update storage URL
        // ******************

        try {
          ArtifactIdentifier artifactId = artifact.getIdentifier();

          // Set the artifact's new storage URL and update the index
          lockArtifact(artifactId);
          artifact.setStorageUrl(makeWarcRecordStorageUrl(dst, warcLength, recordLength).toString());
          getArtifactIndex().updateStorageUrl(artifact.getId(), artifact.getStorageUrl());
          releaseArtifactLock(artifactId);

          log.debug2("Updated storage URL [artifactId: {}, storageUrl: {}]",
              artifact.getId(), artifact.getStorageUrl());

          // Seal active permanent WARC if we've gone over the size threshold
          if (warcLength + recordLength >= getThresholdWarcSize()) {
            sealActiveWarc(artifact.getCollection(), artifact.getAuid(), dst);
          }

          // *****************************
          // Update artifact state journal
          // *****************************

          ArtifactStateEntry state =
              new ArtifactStateEntry(artifact.getIdentifier(), ArtifactState.COPIED);

          // Write new state to journal
          updateArtifactStateJournal(
              getBasePathFromStorageUrl(new URI(artifact.getStorageUrl())),
              artifact.getIdentifier(),
              state);

          log.trace("CopyArtifactTask done: " + getStripe());

        // Thrown by updateStorageUrl call:
        } catch (SolrException | IOException e) {
          // Could not update storage URL so leave its state untouched and allow a re-copy
          if (!isDeleted()) {
            log.error("Error updating storage URL for artifact", e);
            // Q: Is this correct for the reload process?
            return artifact;
          }
        }
      }

      // ********************************
      // Update temporary WARC file stats
      // ********************************

      // tmpWarcFile could be null if this CopyArtifactTask was queued by the reload process
      WarcFile tmpWarcFile = tmpWarcPool.getWarcFile(loc.getPath());

      if (tmpWarcFile != null) {
        synchronized (tmpWarcFile) {
          // This WarcFile cannot have already been GCed because the number of copied
          // has not been incremented yet:
          // If this doesn't happen due to an IOException during copy then it's okay because:
          // 1. It will then never be GCed (counters will not allow that) and so artifact references
          //    will still resolve.
          // 2. Upon restart it should notice there is an artifact pending copy and requeue it.

          tmpWarcFile.getStats().incArtifactsCopied();
        }
      }

      // Save an index lookup by just setting committed to true
      artifact.setCommitted(true);

      return artifact;
    }
  }

  /**
   * Removes an artifact from this data store. Since cutting and splicing WARC files is expensive and there's
   * wariness about actually destroying data, this method currently:
   * <p>
   * 1. Leaves the WARC record in place.
   * 2. Appends the deleted status to the AU's artifact state journal.
   * 3. Removes the artifact reference from the artifact index.
   *
   * @param artifact The {@link Artifact} to remove from this artifact store.
   * @throws IOException
   */
  @Override
  public void deleteArtifactData(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    ArtifactIdentifier artifactId = artifact.getIdentifier();

    lockArtifact(artifactId);

    try {
      // Signal this artifact is deleted to this artifact's the queued copy task if one exists,
      // to avoid a copy into permanent storage and spurious error messages
      CopyArtifactTask queuedTask = queuedCopyTasks.get(artifactId);

      if (queuedTask != null) {
        queuedTask.setDeleted();
      }

      //// Delete artifact reference from the index
      getArtifactIndex().deleteArtifact(artifact.getId());

      //// Mark the artifact as deleted in the artifact state journal
      ArtifactStateEntry state =
          new ArtifactStateEntry(artifact.getIdentifier(), ArtifactState.DELETED);

      // Write new state to journal
      updateArtifactStateJournal(
          getBasePathFromStorageUrl(new URI(artifact.getStorageUrl())),
          artifact.getIdentifier(),
          state);

      //// Update temporary WARC file stats if UNCOMMITTED
      if (getArtifactState(artifact, false) == ArtifactState.UNCOMMITTED) {
        WarcFile tmpWarcFile =
            tmpWarcPool.getWarcFile(getPathFromStorageUrl(new URI(artifact.getStorageUrl())));

        if (tmpWarcFile == null) {
          log.warn("No temporary WARC file in pool for uncommitted artifact [artifact: {}]", artifact);
        } else {
          synchronized (tmpWarcFile) {
            tmpWarcFile.getStats().decArtifactsUncommitted();
          }
        }
      }

      // TODO: Splice out or zero artifact from storage?

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
    } finally {
      releaseArtifactLock(artifactId);
    }

    log.debug("Deleted artifact [artifactId: {}]", artifact.getId());
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

  /**
   * Returns the size in bytes of storage used by this AU. E.g., sum of the sizes of all WARCs in the AU, in
   * {@link WarcArtifactDataStore} implementations.
   *
   * @param collection A {@link String} of the name of the collection containing the AU.
   * @param auid       A {@link String} of the AUID of the AU.
   * @return A {@code long} With the size in bytes of storage space used by this AU.
   */
  @Override
  public long auWarcSize(String collection, String auid) throws IOException {
    return getAuPaths(collection, auid).stream()
        .map(auPath -> findWarcsOrEmpty(auPath))
        .flatMap(Collection::stream)
        // FIXME: Need a better way to exclude journal files
        .filter(path -> !path.endsWith("artifact_state" + WARCConstants.DOT_WARC_FILE_EXTENSION))
        .filter(path -> !path.endsWith("artifact_state" + WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION))
        .mapToLong(this::getWarcLengthOrZero)
        .sum();
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
      Path path = getPathFromStorageUrl(storageUri);

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

  final static String REINDEX_STATE_FILE = "index/reindex";

  /**
   * Rebuilds the provided index from WARCs within this WARC artifact data store.
   * <p>
   * Requirements:
   * * Reindex must preserve ORDER of versions of artifacts, if not exact version
   *
   * @param index The {@code ArtifactIndex} to rebuild and populate from WARCs within this WARC artifact data store.
   * @throws IOException
   */
  @Override
  public void reindexArtifacts(ArtifactIndex index) throws IOException {
    // Path to reindex state file
    Path reindexStatePath = repo.getRepositoryStateDir()
        .toPath()
        .resolve(REINDEX_STATE_FILE);

    File reindexStateFile = reindexStatePath.toFile();

    // Invoke index rebuild from WARCs in this WARC data store
    // if the state file exists
    if (!reindexStateFile.exists()) return;

    // Enable usage of MapDB for large structures
    enableRepoDB();

    // Q: Make this multithreaded? Note: Writes to state file would need to be synchronized
    for (Path basePath : getBasePaths()) {
      indexArtifactsFromWarcs(index, basePath);
    }

    // Disable MapDB
    disableRepoDB();
  }

  String[] REINDEX_STATE_HEADERS = {"start", "end", "indexed", "warc"};

  /**
   * Rebuilds an artifact index from WARCs within this WARC artifact data store.
   *
   * @param index    The {@link ArtifactIndex} to index artifacts into.
   * @param basePath {@link Path} containing a data store base path
   * @throws IOException
   */
  protected void indexArtifactsFromWarcs(ArtifactIndex index, Path basePath) throws IOException {
    if (dataStoreState == DataStoreState.RUNNING) {
      throw new IllegalStateException("Index rebuild only allowed while the data store is stopped");
    }

    log.debug("Reindexing WARCs under data store directory [path: {}]", basePath);

    // List of WARCs that have already been indexed
    List<Path> indexedWarcs = new ArrayList<>();

    // Path to reindex state file
    Path reindexStatePath = repo.getRepositoryStateDir()
        .toPath()
        .resolve(REINDEX_STATE_FILE);

    File reindexStateFile = reindexStatePath.toFile();

    // Read reindex state file as CSV
    try (FileReader reader = new FileReader(reindexStateFile)) {
      Iterable<CSVRecord> records = CSVFormat.DEFAULT
          .withHeader(REINDEX_STATE_HEADERS)
          .withSkipHeaderRecord()
          .parse(reader);

      // Add indexed WARC path to list
      records.forEach(record ->
          indexedWarcs.add(Paths.get(record.get("warc"))));
    }

    // Search under data store base path for WARCs
    Collection<Path> warcPaths = findWarcs(basePath);

    // Find WARCs in permanent storage (exclude journal files, temp WARCs, and processed WARCs)
    Stream<Path> permanentWarcs = warcPaths
        .stream()
        .filter(path -> !isTmpStorage(path))
        .filter(path -> !path.endsWith("lockss-repo" + WARCConstants.DOT_WARC_FILE_EXTENSION))
        .filter(path -> !indexedWarcs.contains(path));

    // Find WARCS in temporary storage
    Stream<Path> temporaryWarcs = warcPaths
        .stream()
        .filter(this::isTmpStorage)
        .filter(path -> !indexedWarcs.contains(path));

    //// Reindex artifacts

    // Process WARCs in permanent storage before WARCs in temporary storage
    try {
      Stream.concat(permanentWarcs, temporaryWarcs)
          .forEach((warcPath) -> {
            try {
              // Reindex artifacts in WARC file
              long start = Instant.now().getEpochSecond();
              long artifactsIndexed = indexArtifactsFromWarc(index, warcPath);
              long end = Instant.now().getEpochSecond();

              // WARC index successful - append record to state file
              // Open writer to state file in append mode
              try (BufferedWriter out = Files.newBufferedWriter(
                  reindexStatePath,
                  StandardOpenOption.APPEND,
                  StandardOpenOption.CREATE)) {

                // Write CSV record
                try (CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
                    .withHeader(REINDEX_STATE_HEADERS)
                    .withSkipHeaderRecord(!indexedWarcs.isEmpty()))) {

                  printer.printRecord(start, end, artifactsIndexed, warcPath);
                }
              } catch (IOException e) {
                log.warn("Could not append record of having indexed WARC file [warc: {}]", warcPath, e);
                // Q: Do something else? The worst that will happen if restarted is reindexArtifactsFromWarc will
                //    be invoked again and iterate over WARC records / artifacts, but it won't index any that are
                //    already indexed.
                //    NOTE: The number of artifacts that were indexed may be lower than the number of artifacts in
                //    the WARC file, if processing of that WARC file was previously interrupted. This does not
                //    necessarily indicate an error.
                return;
              }
            } catch (IOException e) {
              // Thrown by indexArtifactsFromWarc()
              log.error("Error reindexing artifacts from WARC [warc: {}]", warcPath, e);
              throw new LockssUncheckedIOException(e);
            }
          });
    } catch (LockssUncheckedIOException e) {
      // Rethrow IOException thrown by indexArtifactsFromWarc()
      throw e.getIOCause();
    }

    if (!SKIP_INDEXING_IF_MARKED_DELETED) {
      // Paths to journals containing repository state
      Collection<Path> repositoryStateJournals = warcPaths
          .stream()
          .filter(file -> file.endsWith("lockss-repo" + WARCConstants.DOT_WARC_FILE_EXTENSION))
          .collect(Collectors.toList());

      // Replay repository state journal
      for (Path journalPath : repositoryStateJournals) {
        replayArtifactRepositoryStateJournal(index, journalPath);
      }
    }
  }

  /**
   * Iterates over the WARC records in a WARC file and indexes the artifact that the record represents.
   *
   * @param index {@link ArtifactIndex} to index the artifact into.
   * @param warcFile The {@link Path} to a WARC file containing artifacts to index.
   * @return A {@code long} containing the number of artifacts that were indexed. This may be less than the number of
   * artifacts contained in the file WARC file if artifacts were previously indexed or were marked as deleted.
   * @throws IOException
   */
  static boolean SKIP_INDEXING_IF_MARKED_DELETED = true;

  public long indexArtifactsFromWarc(ArtifactIndex index, Path warcFile) throws IOException {
    boolean isWarcInTemp = isTmpStorage(warcFile);
    boolean isCompressed = isCompressedWarcFile(warcFile);

    long artifactsIndexed = 0;

    try (InputStream warcStream = getInputStreamAndSeek(warcFile, 0)) {
      // Get an ArchiveReader from the WARC file input stream
      ArchiveReader archiveReader = getArchiveReader(warcFile, new BufferedInputStream(warcStream));

      // Process each WARC record found by the ArchiveReader
      for (ArchiveRecord record : archiveReader) {
        log.debug(
            "Re-indexing artifact from WARC {} record {} from {}",
            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
            warcFile
        );

        try {
          // Transform ArchiveRecord to ArtifactData
          ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

          if (artifactData != null) {
            // Skip if already indexed
            if (index.artifactExists(artifactData.getIdentifier().getId())) {
              log.debug("Artifact is already indexed [artifactId: {}]", artifactData.getIdentifier().getId());
              continue;
            }

            // Default artifact repository state - the assumption here is that
            // the journal has been lost or incomplete
            artifactData.setArtifactState(ArtifactState.UNCOMMITTED);

            if (SKIP_INDEXING_IF_MARKED_DELETED) {
              try {
                // Determine whether this artifact is recorded as deleted in the journal
                ArtifactStateEntry stateEntry = getArtifactStateEntryFromJournal(artifactData.getIdentifier());

                // Set repository state
                artifactData.setArtifactState(stateEntry.getArtifactState());

                // Do not reindex artifact if it is marked as deleted
                if (stateEntry.isDeleted()) {
                  continue;
                }
              } catch (IOException e) {
                log.warn("Could not read journal file: Applying default repository state to artifact [artifactId: {}]",
                    artifactData.getIdentifier().getId(), e);
              }
            }

            //// Generate storage URL

            // ArchiveRecordHeader#getLength() does not include the pair of CRLFs at the end of every WARC record so
            // we add four bytes to the length
            long recordLength = record.getHeader().getLength() + 4L;
            long compressedRecordLength = 0;

            if (isCompressed) {
              // Read WARC record payload
              record.skip(record.getHeader().getContentLength());

              // Check that the record is at EOF
              if (record.read() > -1) {
                log.warn("Expected an EOF");
              }

              // Set ArchiveReader to EOR
              CompressedWARCReader compressedReader = ((CompressedWARCReader) archiveReader);
              compressedReader.gotoEOR(record);

              // Compute compressed record length using GZIP member boundaries
              compressedRecordLength =
                  compressedReader.getCurrentMemberEnd() - compressedReader.getCurrentMemberStart();
            }

            // Set ArtifactData storage URL
            artifactData.setStorageUrl(makeWarcRecordStorageUrl(warcFile, record.getHeader().getOffset(),
                isCompressed ? compressedRecordLength : recordLength));

            //// Add artifact to the index
            index.indexArtifact(artifactData);
            artifactsIndexed++;
          }
        } catch (IOException e) {
          log.error("Could not index artifact from WARC record [artifactId: {}, warcFile: {}]",
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
              warcFile, e);

          throw e;
        }
      }
    } catch (IOException e) {
      log.error("Could not open WARC file [warcFile: {}]", warcFile, e);
      throw e;
    }

    // Return the number of artifacts indexed from this WARC file
    return artifactsIndexed;
  }

  // *******************************************************************************************************************
  // * JOURNAL OPERATIONS
  // *******************************************************************************************************************

  protected SemaphoreMap<ArchivalUnitStem> auLocks = new SemaphoreMap<>();

  private static class ArchivalUnitStem {
    private final String collection;
    private final String auid;

    public ArchivalUnitStem(String collection, String auid) {
      this.collection = collection;
      this.auid = auid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ArchivalUnitStem that = (ArchivalUnitStem) o;
      return collection.equals(that.collection) && auid.equals(that.auid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(collection, auid);
    }
  }

  /**
   * Updates the repository state of artifact by appending an entry to the repository state journal in the
   * artifact's AU.
   *
   * @param basePath   A {@link Path} containing the base path of the artifact.
   * @param artifactId The {@link ArtifactIdentifier} of the artifact to update.
   * @param state      The new {@link ArtifactStateEntry}.
   * @return The {@link ArtifactStateEntry} that was recorded.
   * @throws IOException
   */
  // TODO: Generalize this to arbitrary metadata
  public ArtifactStateEntry updateArtifactStateJournal(
      Path basePath,
      ArtifactIdentifier artifactId,
      ArtifactStateEntry state
  ) throws IOException {

    Objects.requireNonNull(basePath, "A repository base path must be provided");
    Objects.requireNonNull(artifactId, "Artifact identifier is null");
    Objects.requireNonNull(state, "Repository artifact metadata is null");

    ArchivalUnitStem auStem = new ArchivalUnitStem(artifactId.getCollection(), artifactId.getAuid());

    try {
      auLocks.getLock(auStem);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while waiting to acquire AU lock");
    }

    Path auJournalPath = getAuJournalPath(basePath, artifactId.getCollection(), artifactId.getAuid(),
        ArtifactStateEntry.LOCKSS_JOURNAL_ID);

    log.trace("auJournalPath = {}", auJournalPath);

    try {
      // Append an entry (a WARC metadata record) to the journal
      try (OutputStream output = initWarcAndGetAppendableOutputStream(auJournalPath)) {
        WARCRecordInfo journalRecord = createWarcMetadataRecord(artifactId.getId(), state);
        writeWarcRecord(journalRecord, output);
      }
    } finally {
      auLocks.releaseLock(auStem);
    }

    log.debug2("Updated artifact repository state [artifactId: {}, state: {}]", artifactId, state);

    return state;
  }

  /**
   * Returns an appendable {@link OutputStream} or initializes the WARC first if a {@link FileNotFoundException}
   * is thrown trying to open it.
   */
  private OutputStream initWarcAndGetAppendableOutputStream(Path warcPath) throws IOException {
    try {
      return getAppendableOutputStream(warcPath);
    } catch (FileNotFoundException e) {
      initWarc(warcPath);
      return getAppendableOutputStream(warcPath);
    }
  }

  /**
   * Positions of flags in the artifact state bit-vector.
   */
  private static int INDEX_DELETED = 0;
  private static int INDEX_COMMITTED = 1;

  /**
   * Name of MapDB map from artifact ID to bit-encoded {@link ArtifactStateEntry}.
   */
  private static String ARTIFACT_REPOSTATES_MAP_NAME = "artifact-repostates";

  /**
   * Handle to MapDB instance.
   */
  private DB repodb;

  /**
   * Handle to the MapDB {@link HTreeMap} map from artifact ID to a bit-encoded {@link ArtifactStateEntry}.
   */
  HTreeMap<String, long[]> flagsMap;

  /**
   * Enables use of a MapDB instance for potentially large structures, by creating a new one backed by temporary
   * files.
   */
  protected void enableRepoDB() throws IOException {
    if (repodb != null) {
      log.warn("Already using MapDB");
      return;
    }

    // Location of MapDB instance in temporary storage
    File tmpMapDBBaseDir = FileUtil.createTempDir("tmpMapDB", null);
    File tmpMapDB = new File(tmpMapDBBaseDir, "mapdb");

    // Create a DB instance from DBMaker
    repodb = (DBMaker.fileDB(tmpMapDB).fileDeleteAfterClose()).make();

    // Create a MapDB hash map from artifact ID to bit-encoded artifact repository state
    flagsMap = repodb.hashMap(ARTIFACT_REPOSTATES_MAP_NAME)
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.LONG_ARRAY)
        .create();
  }

  /**
   * Disables using a MapDB instance for potentially large structures.
   */
  protected void disableRepoDB() {
    // Close MapDB hash map
    if (flagsMap != null) {
      flagsMap.close();
      flagsMap = null;
    }

    // Close MapDB
    if (repodb != null) {
      repodb.close();
      repodb = null;
    }
  }

  /**
   * Adds an entry to the MapDB hash map from an artifact ID to that artifact's {@link ArtifactStateEntry}.
   *
   * @param state
   */
  private void addArtifactStateToMap(ArtifactStateEntry state) {
    if (flagsMap == null) {
      throw new IllegalStateException("MapDB not enabled");
    }

    // Encode artifact repository state as a bit vector
    BitSet flags = new BitSet();

    if (state.isDeleted())
      flags.set(INDEX_DELETED);

    if (state.isCommitted())
      flags.set(INDEX_COMMITTED);

    // Encode the bit vector as a long array and put it on the map
    flagsMap.put(state.getArtifactId(), flags.toLongArray());
  }

  /**
   * Returns the {@link ArtifactStateEntry} of an artifact from the MapDB instance.
   *
   * @param artifactId
   * @return
   */
  @Deprecated
  private ArtifactStateEntry getArtifactStateFromMap(ArtifactIdentifier artifactId) {
    if (flagsMap == null) {
      throw new IllegalStateException("MapDB not enabled");
    }

    // The long array representing the bit vector
    long[] encodedFlags = flagsMap.get(artifactId.getId());

    if (encodedFlags == null)
      return null;

    log.trace("encodedFlags = {}", encodedFlags);

    // Convert long array to BitSet
    BitSet flags = BitSet.valueOf(encodedFlags);

    ArtifactState state = ArtifactState.UNKNOWN;

    if (flags.get(INDEX_COMMITTED))
      state = ArtifactState.PENDING_COPY;

    if (flags.get(INDEX_DELETED))
      state = ArtifactState.DELETED;

    // Create a new ArtifactRepositoryState from BitSet
    return new ArtifactStateEntry(artifactId, state);
  }

  /**
   * Truncates a journal by rewriting it with only its most recent entry per artifact ID.
   *
   * @param journalPath A {@link Path} containing the path to the data store journal to truncate.
   * @throws IOException
   */
  protected void truncateAuJournalFile(Path journalPath) throws IOException {
    // Get latest entry per artifact ID
    List<ArtifactStateEntry> journalEntries = readJournal(journalPath, ArtifactStateEntry.class); // FIXME

    // Replace the journal with a new file
    removeWarc(journalPath); // FIXME
    initWarc(journalPath);

    // Write journal with only latest entries
    try (OutputStream output = getAppendableOutputStream(journalPath)) {
      for (ArtifactStateEntry journalEntry : journalEntries) {
        // Append WARC metadata record to the new journal
        WARCRecordInfo metadataRecord = createWarcMetadataRecord(journalEntry.getArtifactId(), journalEntry);
        writeWarcRecord(metadataRecord, output);
      }
    }
  }

  /**
   * Reads an artifact's current repository state from storage.
   *
   * @param aid An {@link ArtifactIdentifier}
   * @return The {@link ArtifactStateEntry} of the artifact.
   * @throws IOException
   */
  protected ArtifactStateEntry getArtifactStateEntryFromJournal(ArtifactIdentifier aid) throws IOException {
    if (aid == null) {
      throw new IllegalArgumentException("Null artifact identifier");
    }

    //// Return artifact state from repository database (if enabled)
    ArtifactStateEntry result = getArtifactStateFromMap(aid);

    if (result != null) {
      return result;
    }

    //// Read artifact state from journal
    Map<String, ArtifactStateEntry> artifactStates = new HashMap<>();

    for (Path journalPath :
        getAuJournalPaths(aid.getCollection(), aid.getAuid(), ArtifactStateEntry.LOCKSS_JOURNAL_ID)) {

      // Get journal entries from file
      List<ArtifactStateEntry> journal = readJournal(journalPath, ArtifactStateEntry.class);

      for (ArtifactStateEntry journalEntry : journal) {

        // Get existing state from map
        ArtifactStateEntry state = artifactStates.get(journalEntry.getArtifactId());

        Instant journalEntryDate = Instant.ofEpochMilli(journalEntry.getEntryDate());
        Instant latestEntryDate = Instant.ofEpochMilli(state == null ? 0 : state.getEntryDate());

        if (state == null ||
            journalEntryDate.equals(latestEntryDate) || journalEntryDate.isAfter(latestEntryDate)) {

          // Update latest journal entry map
          artifactStates.put(journalEntry.getArtifactId(), journalEntry);
        }
      }
    }

    // Update the MapDB HashMap
    if (flagsMap != null) {
      artifactStates.values()
          .forEach(state -> addArtifactStateToMap(state));
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
  protected <T> List<T> readJournal(Path journalPath, Class<T> journalEntryClass) throws IOException {
    List<T> journalEntries = new ArrayList<>();

    log.trace("journalPath = {}", journalPath);

    try (InputStream warcStream = getInputStreamAndSeek(journalPath, 0)) {
      // FIXME: Move this to constructor
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      for (ArchiveRecord record : getArchiveReader(journalPath, new BufferedInputStream(warcStream))) {
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
   * Reads and replays repository state to a given artifact index.
   *
   * @param index       An {@code ArtifactIndex} to replay repository state to.
   * @param journalPath A {@code String} containing the path to a repository state journal WARC file.
   * @throws IOException
   */
  protected void replayArtifactRepositoryStateJournal(ArtifactIndex index, Path journalPath) throws IOException {
    for (ArtifactStateEntry state : readJournal(journalPath, ArtifactStateEntry.class)) {
      // Get the artifact ID of this repository state
      String artifactId = state.getArtifactId();

      log.trace("artifactState = {}", state);

      log.debug("Replaying repository state for artifact {} from repository state file {}",
          artifactId,
          journalPath
      );

      log.debug("state = {}", state);

      // Replay to artifact index
      if (index.artifactExists(artifactId)) {
        if (state.isDeleted()) {
          log.debug2("Removing deleted artifact from index [artifactId: {}]", artifactId);
          index.deleteArtifact(artifactId);
          continue;
        }

        if (state.isCommitted()) {
          log.debug2("Marking artifact as committed in index [artifactId: {}]", artifactId);
          index.commitArtifact(artifactId);
        }
      } else {
        // This is not necessarily an error e.g., for deleted artifacts that
        // were not re-indexed in the first place. But we cannot
        log.debug2(
            "Cannot apply state: Artifact missing from index [artifactId: {}, state: {}]",
            artifactId, state);
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

    //// Mandatory WARC record headers

    // Set WARC-Record-ID
    record.setRecordId(URI.create(artifactId.getId()));

    // Set WARC record type
    record.setType(WARCRecordType.response);

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
            fetchTime < 0 ?
                Instant.ofEpochMilli(TimeBase.nowMs()).atZone(ZoneOffset.UTC) :
                Instant.ofEpochMilli(fetchTime).atZone(ZoneOffset.UTC)));

    //// Optional WARC record headers

    // Set WARC record URL
    record.setUrl(artifactId.getUri());

    // Set WARC record Content-Type - hardcoded to always write an HTTP response
    // containing the artifact headers/properties and data
    record.setMimetype("application/http; msgtype=response");

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
                Instant.now().atZone(ZoneOffset.UTC)));

    byte[] headers = ArtifactDataUtil.getHttpResponseHeader(artifactData);

    try {
      // Assumption is artifact (WARC payload) length and digest have been set by the caller

      // WARC block length and stream
      record.setContentLength(headers.length + artifactData.getContentLength());
      record.setContentStream(
          ArtifactDataUtil.getHttpResponseStreamFromHttpResponse(
              ArtifactDataUtil.getHttpResponseFromArtifactData(artifactData)));

    } catch (IllegalStateException e) {
      // Compute length and digest by exhausting the InputStream
      try (DeferredTempFileOutputStream dfos =
               new DeferredTempFileOutputStream((int) DEFAULT_DFOS_THRESHOLD, "compute-length")) {

        artifactData.setComputeDigestOnRead(true);

        // Create an HTTP response stream from the ArtifactData
        InputStream httpResponse =
            ArtifactDataUtil.getHttpResponseStreamFromHttpResponse(
                ArtifactDataUtil.getHttpResponseFromArtifactData(artifactData));

        IOUtils.copyLarge(httpResponse, dfos);

        // WARC block length and stream
        record.setContentLength(dfos.getByteCount());
        record.setContentStream(dfos.getDeleteOnCloseInputStream());

        String contentDigest = String.format("%s:%s",
            artifactData.getMessageDigest().getAlgorithm(),
            new String(Hex.encodeHex(artifactData.getMessageDigest().digest())));

        // Artifact (i.e., WARC payload) length and digest
        artifactData.setContentLength(artifactData.getBytesRead());
        artifactData.setContentDigest(contentDigest);
      }
    }

    // Set WARC payload (artifact) length (i.e., WARC block length minus HTTP headers)
    record.addExtraHeader(ArtifactConstants.ARTIFACT_LENGTH_KEY,
        String.valueOf(record.getContentLength() - headers.length));

    // Set WARC-Payload-Digest and our custom artifact digest key. Both represent the digest of the
    // artifact data. Our custom artifact digest key is added here for backward compatibility.
    record.addExtraHeader(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST, artifactData.getContentDigest());
    record.addExtraHeader(ArtifactConstants.ARTIFACT_DIGEST_KEY, artifactData.getContentDigest());

    // TODO: Set WARC-Block-Digest header
    // record.addExtraHeader(WARCConstants.HEADER_KEY_BLOCK_DIGEST, artifactData.getHttpResponseDigest());

    // Write record to output stream and return number of bytes written
    try (CountingOutputStream cout = new CountingOutputStream(outputStream)) {
      writeWarcRecord(record, cout);
      return cout.getCount();
    } finally {
      IOUtils.closeQuietly(record.getContentStream());
    }
  }

  /**
   * This circumvents an issues in IIPC's webarchive-commons library.
   * <p>
   * See {@link SimpleRepositionableStream} for details.
   */
  protected ArchiveReader getArchiveReader(Path warcFile, InputStream input) throws IOException {
    return isCompressedWarcFile(warcFile) ?
        new CompressedWARCReader(warcFile.getFileName().toString(), input) :
        new UncompressedWARCReader(warcFile.getFileName().toString(), input);
  }

  /**
   * This circumvents another issue in IIPC's webarchive-commons library:
   * <p>
   * {@code ArchiveReader.ArchiveRecordIterator} requires an {@link InputStream} that supports
   * {@link InputStream#mark(int)} which {@link GZIPInputStream} does not support. Wrapping it in a
   * {@link BufferedInputStream} causes problems because {@link ArchiveReader#positionForRecord(InputStream)} expects
   * either an {@link GZIPInputStream} or attempts to cast anything else as a {@link CountingInputStream}.
   */
  public static class CompressedWARCReader extends WARCReader {
    public CompressedWARCReader(final String f, final InputStream is) throws IOException {
      GZIPMembersInputStream gmis = new GZIPMembersInputStream(is);
      gmis.setEofEachMember(true);

      setIn(gmis);
      setCompressed(true);
      initialize(f);
    }

    public long getCurrentMemberStart() {
      return ((GZIPMembersInputStream) in).getCurrentMemberStart();
    }

    public long getCurrentMemberEnd() {
      return ((GZIPMembersInputStream) in).getCurrentMemberEnd();
    }

    /**
     * Circumvents a bug in WARC record length calculation. See {@link SimpleRepositionableStream} for details.
     */
    @Override
    protected WARCRecord createArchiveRecord(InputStream is, long offset) throws IOException {
      return (WARCRecord) currentRecord(new WARCRecord(new SimpleRepositionableStream(is), getReaderIdentifier(),
          offset, isDigest(), isStrict()));
    }

    /**
     * COPIED FROM WEBARCHIVE-COMMONS
     * <p>
     * Get record at passed <code>offset</code>.
     *
     * @param offset Byte index into file at which a record starts.
     * @return A WARCRecord reference.
     * @throws IOException
     */
    public WARCRecord get(long offset) throws IOException {
      cleanupCurrentRecord();
      ((GZIPMembersInputStream) getIn()).compressedSeek(offset);
      return (WARCRecord) createArchiveRecord(getIn(), offset);
    }

    /**
     * COPIED FROM WEBARCHIVE-COMMONS
     *
     * @return
     */
    public Iterator<ArchiveRecord> iterator() {
      /**
       * Override ArchiveRecordIterator so can base returned iterator on
       * GzippedInputStream iterator.
       */
      return new ArchiveRecordIterator() {
        private GZIPMembersInputStream gis = (GZIPMembersInputStream) getIn();

        private Iterator<GZIPMembersInputStream> gzipIterator = this.gis.memberIterator();

        protected boolean innerHasNext() {
          return this.gzipIterator.hasNext();
        }

        protected ArchiveRecord innerNext() throws IOException {
          // Get the position before gzipIterator.next moves
          // it on past the gzip header.
          InputStream is = (InputStream) this.gzipIterator.next();
          return createArchiveRecord(is, Math.max(gis.getCurrentMemberStart(), gis.getCurrentMemberEnd()));
        }
      };
    }

    /**
     * COPIED FROM WEBARCHIVE-COMMONS
     *
     * @return
     */
    protected void gotoEOR(ArchiveRecord rec) throws IOException {
      long skipped = 0;
      while (getIn().read() > -1) {
        skipped++;
      }
      if (skipped > 4) {
        System.err.println("unexpected extra data after record " + rec);
      }
      return;
    }
  }

  /**
   * Circumvents an bug in WARC record length calculation. See {@link SimpleRepositionableStream} for details.
   */
  public static class UncompressedWARCReader extends WARCReader {
    public UncompressedWARCReader(final String f, final InputStream is) throws IOException {
      setIn(new CountingInputStream(is));
      initialize(f);
    }

    /**
     * Circumvents an bug in WARC record length calculation. See {@link SimpleRepositionableStream} for details.
     */
    @Override
    protected WARCRecord createArchiveRecord(InputStream is, long offset) throws IOException {
      return (WARCRecord) currentRecord(new WARCRecord(new SimpleRepositionableStream(is), getReaderIdentifier(),
          offset, isDigest(), isStrict()));
    }
  }

  /**
   * Creates a WARCRecordInfo object representing a WARC metadata record with a JSON object as its payload.
   *
   * @param refersTo The WARC-Record-Id of the WARC record this metadata is attached to (i.e., for WARC-Refers-To).
   * @param state    A RepositoryArtifactMetadata with the artifact metadata.
   * @return A WARCRecordInfo representing the given artifact metadata.
   */
  // FIXME Make this generic
  public static WARCRecordInfo createWarcMetadataRecord(String refersTo, ArtifactStateEntry state) throws IOException {
    // Create a WARC record object
    WARCRecordInfo record = new WARCRecordInfo();

    // Set record content stream
    ObjectMapper mapper = new ObjectMapper();
    byte[] jsonBytes = mapper.writeValueAsBytes(state);
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

      // Sanity check: The number of bytes read from the WARC record should match its declared Content-Length
      if (bytesWritten != record.getContentLength()) {
        log.warn(
            "Number of bytes written did not match Content-Length header (expected: {} bytes, wrote: {} bytes)",
            record.getContentLength(),
            bytesWritten);
      }
    }

    // Write the two CRLF blocks required at end of every record (per the spec)
    out.write(TWO_CRLF_BYTES);
    out.flush();
  }

  public static void writeWarcRecordHeader(WARCRecordInfo record, OutputStream out) throws IOException {
    // Write the header
    out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

    // Write a CRLF block to separate header from body
    out.write(CRLF_BYTES);
    out.flush();
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
  public static WARCRecordInfo createWarcInfoRecord(MultiValueMap<String, String> headers, MediaType mimeType,
                                                    byte[] content) {
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
