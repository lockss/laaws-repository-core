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
import org.apache.commons.io.output.ByteArrayOutputStream;
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
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.io.RepoAuid;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.laaws.rs.util.*;
import org.lockss.log.L4JLogger;
import org.lockss.util.CloseCallbackInputStream;
import org.lockss.util.concurrent.stripedexecutor.StripedCallable;
import org.lockss.util.concurrent.stripedexecutor.StripedExecutorService;
import org.lockss.util.concurrent.stripedexecutor.StripedRunnable;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.jms.JmsConsumer;
import org.lockss.util.jms.JmsFactory;
import org.lockss.util.jms.JmsUtil;
import org.lockss.util.time.TimeUtil;
import org.lockss.util.time.TimerUtil;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StreamUtils;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This abstract class aims to capture operations that are common to all ArtifactDataStore implementations
 * that serialize ArtifactData as WARC records in a WARC file.
 */
public abstract class WarcArtifactDataStore implements ArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata>, WARCConstants {
  private final static L4JLogger log = L4JLogger.getLogger();

  // DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS") does not parse in Java 8: https://bugs.openjdk.java.net/browse/JDK-8031085
  protected static final DateTimeFormatter FMT_TIMESTAMP =
      new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss")
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withZone(ZoneId.of("UTC"));

  protected static final String WARC_FILE_EXTENSION = "warc";
  protected static final String AU_DIR_PREFIX = "au-";
  protected static final String AU_ARTIFACTS_WARC_NAME = "artifacts" + "." + WARC_FILE_EXTENSION;

  protected static final String COLLECTIONS_DIR = "collections";
  protected static final String TMP_WARCS_DIR = "temp";

  protected static final String WARCID_SCHEME = "urn:uuid";
  protected static final String CRLF = "\r\n";
  protected static byte[] CRLF_BYTES;

  public final static String DEFAULT_TMPWARCBASEPATH = TMP_WARCS_DIR;

  private static final long DEFAULT_DFOS_THRESHOLD = 16L * FileUtils.ONE_MB;

  protected static final String ENV_THRESHOLD_WARC_SIZE = "REPO_MAX_WARC_SIZE";
  protected static final long DEFAULT_THRESHOLD_WARC_SIZE = 1L * FileUtils.ONE_GB;
  protected long thresholdWarcSize;

  protected static final String ENV_UNCOMMITTED_ARTIFACT_EXPIRATION = "REPO_UNCOMMITTED_ARTIFACT_EXPIRATION";
  protected static final long DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION = TimeUtil.WEEK;
  protected long uncommittedArtifactExpiration;

  protected ArtifactIndex artifactIndex;
  protected LockssRepository lockssRepo;
  protected WarcFilePool tmpWarcPool;
  protected Map<RepoAuid, List<Path>> auActiveWarcMap;
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

  /**
   * The interval to wait between consecutive retries when creating the JMS
   * consumer.
   */
  private long retryDelay = DEFAULT_RETRY_DELAY;

  /**
   * Delay between retries when creating the JMS consumer.
   */
  public static final long DEFAULT_RETRY_DELAY = 1 * TimeUtil.SECOND;
  protected static String CLIENT_ID = "ArtifactDataStore";
  protected static String JMS_TOPIC = "AuEventTopic";
  protected JmsConsumer jmsConsumer;

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

  /**
   * Returns the base path containing temporary WARCs.
   *
   * @return A {@code String} containing the base path for temporary WARCs.
   */
  protected abstract String makeStorageUrl(Path filePath, MultiValueMap<String, String> params);

  protected abstract InputStream getInputStreamAndSeek(Path filePath, long seek) throws IOException;

  protected abstract OutputStream getAppendableOutputStream(Path filePath) throws IOException;

  protected abstract void initWarc(Path warcPath) throws IOException;

  protected abstract long getWarcLength(Path warcPath) throws IOException;

  protected abstract Collection<Path> findWarcs(Path basePath) throws IOException;

  protected abstract boolean removeWarc(Path warcPath) throws IOException;

  protected abstract long getBlockSize();

  // *******************************************************************************************************************
  // * ABSTRACT PATH METHODS
  // *******************************************************************************************************************

//  protected abstract boolean isCollectionsBase(Path storageUrl);
//  protected abstract boolean isTmpStorage(Path storageUrl);
//  protected abstract Path[] getTmpWarcBasePaths();
//  protected abstract Path getAuActiveWarcPath(String collectionId, String auId);
//  protected abstract Path[] getAuActiveWarcPaths(String collectionId, String auId);
//  protected abstract Path getAuMetadataWarcPath(ArtifactIdentifier aid, String journalName);
//  protected abstract Path[] getAuMetadataWarcPaths(ArtifactIdentifier aid, String journalName);

  protected boolean isCollectionsBase(Path storageUrl) {
    return true;
  }

  protected boolean isTmpStorage(Path storageUrl) {
    return Arrays.stream(getTmpWarcBasePaths())
        .map(basePath -> storageUrl.startsWith(basePath))
        .anyMatch(Predicate.isEqual(true));
  }

  protected Path[] getTmpWarcBasePaths() {
    Path[] basePaths = getBasePaths();

    if (basePaths == null) {
      throw new IllegalStateException("No base paths");
    }

    List<Path> paths = Arrays.stream(basePaths)
        .map(base -> base.resolve(TMP_WARCS_DIR))
        .collect(Collectors.toList());

    log.trace("paths = {}", paths);

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
  }

  public Path[] getCollectionTmpWarcsPaths(String collectionId) {
    List<Path> paths = Arrays.stream(getCollectionPaths(collectionId))
        .map(path -> path.resolve(TMP_WARCS_DIR))
        .collect(Collectors.toList());

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
  }

  public Path[] getCollectionPaths(String collectionId) {
    List<Path> paths = Arrays.stream(getCollectionsBase())
        .map(path -> path.resolve(collectionId))
        .collect(Collectors.toList());

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
  }

  public Path[] getCollectionsBase() {
    List<Path> paths = Arrays.stream(getBasePaths())
        .map(path -> path.resolve(COLLECTIONS_DIR))
        .collect(Collectors.toList());

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
  }

  public Path getAuActiveWarcPath(String collectionId, String auId) throws IOException {
    // TODO Determine which active WARC to write to based on free space and
    Path[] auPaths = getAuActiveWarcPaths(collectionId, auId);
    log.trace("auPaths = {}", auPaths);

    Path auPath = Arrays.stream(auPaths)
        .sorted((a, b) -> (int) (a.toFile().getFreeSpace() - b.toFile().getFreeSpace()))
        .findFirst()
        .orElse(null);

    return auPath == null ? initAuActiveWarc(collectionId, auId) : auPath;
  }

  protected Path initAuActiveWarc(String collection, String auid) throws IOException {
//    initAu(collection, auid);

    Path[] auPaths = getAuPaths(collection, auid);
    log.trace("auPaths = {}", auPaths);

    Path warcFile = auPaths[0].resolve(generateActiveWarcName(collection, auid));
//    initWarc(warcFile);


    RepoAuid aukey = new RepoAuid(collection, auid);

    synchronized (auActiveWarcMap) {
      List<Path> paths = auActiveWarcMap.getOrDefault(aukey, new ArrayList<>());
      paths.add(warcFile);
      auActiveWarcMap.put(aukey, paths);
    }

    return warcFile;
  }

  public Path[] getAuActiveWarcPaths(String collectionId, String auId) {
    // Key into the active WARC map
    RepoAuid au = new RepoAuid(collectionId, auId);

    synchronized (auActiveWarcMap) {
      List<Path> paths = auActiveWarcMap.get(au);

      if (paths == null) {
        return new Path[]{};
      }

      return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
    }
  }

  protected Path getAuMetadataWarcPath(ArtifactIdentifier aid, String journalName) {
    return getAuMetadataWarcPaths(aid, journalName)[0];
  }

  protected Path[] getAuMetadataWarcPaths(ArtifactIdentifier aid, String journalName) {
    List<Path> paths = Arrays.stream(getAuPaths(aid.getCollection(), aid.getAuid()))
        .map(path -> path.resolve(journalName + "." + WARC_FILE_EXTENSION))
        .collect(Collectors.toList());

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
  }

  public Path[] getAuPaths(String collectionId, String auid) {
    List<Path> paths = Arrays.stream(getCollectionPaths(collectionId))
        .map(path -> path.resolve(AU_DIR_PREFIX + DigestUtils.md5Hex(auid)))
        .collect(Collectors.toList());

    return Arrays.copyOf(paths.toArray(), paths.toArray().length, Path[].class);
  }

  /**
   * Returns the base path of this LOCKSS repository.
   *
   * @return A {@code String} containing the base path of this LOCKSS repository.
   */
  public Path[] getBasePaths() {
    return basePaths;
  }

  public static Path[] basePaths;

  // *******************************************************************************************************************
  // * CONSTRUCTORS
  // *******************************************************************************************************************

  /**
   * Constructor for a WARC artifact data store.
   *
   */
  public WarcArtifactDataStore(ArtifactIndex index) {
    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    this.stripedExecutor = new StripedExecutorService();

    this.auActiveWarcMap = new HashMap<>();

    this.artifactIndex = index;

    setThresholdWarcSize(NumberUtils.toLong(System.getenv(ENV_THRESHOLD_WARC_SIZE), DEFAULT_THRESHOLD_WARC_SIZE));

    setUncommittedArtifactExpiration(NumberUtils.toLong(
        System.getenv(ENV_UNCOMMITTED_ARTIFACT_EXPIRATION),
        DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION
    ));

    makeJmsConsumer();
  }

  // *******************************************************************************************************************
  // * STARTUP
  // *******************************************************************************************************************

  @Override
  public void initDataStore() {
    log.debug2("Initializing data store");

    reloadDataStoreState();
    dataStoreState = DataStoreState.INITIALIZED;

    log.info("Initialized data store");
  }

  /**
   * Schedules an asynchronous reload of the data store state.
   */
  protected void reloadDataStoreState() {
    stripedExecutor.submit(new ReloadDataStoreStateTask());
  }

  protected void runGarbageCollector() {
    scheduledExecutor.submit(new GarbageCollectTempWarcsTask());
  }

  private class ReloadDataStoreStateTask implements Runnable {
    @Override
    public void run() {

      try {
        // Reload temporary WARCs
        reloadTemporaryWarcs();
      } catch (Exception e) {
        log.error("Could not reload data store state: {}", e);
        throw new IllegalStateException(e);
      }

      // Schedule temporary WARC garbage collection
      // TODO: Parameterize interval
      scheduledExecutor.scheduleAtFixedRate(new GarbageCollectTempWarcsTask(), 0, 1, TimeUnit.DAYS);
      log.info("Scheduled temporary WARC garbage collector");
    }
  }

  private class GarbageCollectTempWarcsTask implements Runnable {
    @Override
    public void run() {
      garbageCollectTempWarcs();
    }
  }

  // *******************************************************************************************************************
  // * METHODS
  // *******************************************************************************************************************

  /**
   *
   *
   * @throws IOException
   */
  protected void garbageCollectTempWarcs() {
    Path[] tmpWarcBasePaths = getTmpWarcBasePaths();

    log.debug("Starting GC of temporary files [tmpWarcBasePaths: {}]", tmpWarcBasePaths);

    Arrays.stream(tmpWarcBasePaths).forEach(this::garbageCollectTempWarcs);
  }

  protected void garbageCollectTempWarcs(Path tmpWarcBasePaths) {
    try {
      Collection<Path> tmpWarcs = findWarcs(tmpWarcBasePaths);

      log.debug("Found {} temporary WARCs under {}: {}", tmpWarcs.size(), tmpWarcBasePaths, tmpWarcs);

      for (Path tmpWarcPath : tmpWarcs) {
        WarcFile warcFile = null;

        // Remove the temporary WARC from the pool if necessary
        synchronized (tmpWarcPool) {
          if (tmpWarcPool.isInUse(tmpWarcPath)) {
            log.debug2("Temporary WARC file is in use; will attempt a GC again later");
            continue;
          } else if (tmpWarcPool.isInPool(tmpWarcPath)) {
            warcFile = tmpWarcPool.lookupWarcFile(tmpWarcPath);
            if (!tmpWarcPool.borrowWarcFile(tmpWarcPath)) {
              log.debug2(
                  "Could not borrow temporary WARC file; will attempt a GC again later [tmpWarcPath: {}]",
                  tmpWarcPath
              );
              continue;
            }
          } else {
            log.warn("Temporary WARC not found in pool [tmpWarcPath: {}]", tmpWarcPath);
            continue;
          }
        }

        // Determine whether this temporary WARC should be removed
        try {
          if (isTempWarcRemovable(tmpWarcPath)) {
            // YES - Remove the temporary WARC from storage
            log.debug2("Removing temporary WARC file [tmpWarcPath: {}]", tmpWarcPath);
            tmpWarcPool.removeWarcFile(warcFile);
            removeWarc(tmpWarcPath);
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
              tmpWarcPath,
              e
          );
        }
      }

    } catch (IOException e) {
      log.error(
          "Caught IOException while trying to find temporary WARC files [tmpWarcBasePath: {}]: {}",
          tmpWarcBasePaths,
          e
      );
    }

    log.debug("Finished GC of temporary WARC files [tmpWarcBasePath: {}]", tmpWarcBasePaths);
  }

  /**
   * Convenience method that encodes the location and length of a WARC record into an internal URL.
   *
   * @param filePath A {@code String} containing the path to the WARC file containing the WARC record.
   * @param offset A {@code long} containing the byte offset from the beginning of this WARC file to the beginning of
   *               the WARC record.
   * @param length A {@code length} containing the length of the WARC record.
   * @return A {@code String} containing the internal storage URL to the WARC record.
   */
  protected String makeStorageUrl(Path filePath, long offset, long length) {
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add("offset", Long.toString(offset));
    params.add("length", Long.toString(length));
    return makeStorageUrl(filePath, params);
  }

  /**
   * Convenience method that returns an {@code InputStream} containing the entire WARC file at the requested path.
   *
   * @param path A {@code String} containing the path of the WARC file.
   * @return An {@code InputStream} containing the WARC file.
   * @throws IOException
   */
  protected InputStream getInputStream(Path path) throws IOException {
    return getInputStreamAndSeek(path, 0L);
  }

  /**
   * Reads and reloads state from temporary WARCs, including the requeuing of copy tasks of committed artifacts from
   * temporary to permanent storage. Removes temporary WARCs if eligible:
   *
   * A temporary WARC may be removed if the records contained within it are the serializations of artifacts that are
   * either uncommitted-but-expired or committed-and-moved-to-permanent-storage.
   */
  protected void reloadTemporaryWarcs() throws IOException {
    if (artifactIndex == null) {
      throw new IllegalStateException("Cannot reload data store state from temporary WARCs without an artifact index");
    }

    Path[] tmpWarcBasePath = getTmpWarcBasePaths();

    log.info("Reloading temporary WARCs from {}", tmpWarcBasePath);

    for (Path path : tmpWarcBasePath) {
      reloadTemporaryWarcs(path);
    }
  }

  protected void reloadTemporaryWarcs(Path tmpWarcBasePath) throws IOException {

    Collection<Path> tmpWarcs = findWarcs(tmpWarcBasePath);
    log.debug("Found {} temporary WARCs: {}", tmpWarcs.size(), tmpWarcs);

    // Iterate over the temporary WARC files that were found
    for (Path tmpWarc : tmpWarcs) {
      boolean isTmpWarcRemovable = true;

      log.debug2("tmpWarc = {}", tmpWarc);

      if (tmpWarcPool.isInPool(tmpWarc)) {
        log.debug2("Temporary file is already in pool - skipping reload [tmpWarc: {}]", tmpWarc);
        continue;
      }

//      if (TempWarcInUseTracker.INSTANCE.isInUse(tmpWarc)) {
//        log.warn("Temporary file is in use - skipping reload [tmpWarc: {}]", tmpWarc);
//        continue;
//      }

      //// Handle WARC records in this temporary WARC file
      try (InputStream warcStream = markAndGetInputStream(tmpWarc)) {
        // Get an ArchiveReader Iterable over ArchiveRecord objects
        ArchiveReader archiveReader = new UncompressedWARCReader(tmpWarc.toString(), warcStream);

        // Iterate over the WARC records
        for (ArchiveRecord record : archiveReader) {
          boolean isRecordRemovable = false;

          // ArchiveRecordHeader#getLength() does not include the pair of CRLFs at the end of every WARC record so
          // we add four bytes to the length
          long recordLength = record.getHeader().getLength() + 4L;

          ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);
          String artifactId = artifactData.getIdentifier().getId();
          artifactData.setStorageUrl(makeStorageUrl(tmpWarc, record.getHeader().getOffset(), recordLength));

          log.trace("artifactData.getStorageUrl() = {}", artifactData.getStorageUrl());

          // Handle the different life cycle states in which the artifact may be.
          switch (
              getArtifactState(
                  isArtifactExpired(record),
                  isArtifactDeleted(artifactData.getIdentifier()),
                  artifactIndex.getArtifact(artifactId))
          ) {
            case NOT_INDEXED:
              // Reindex artifact
              log.warn("Could not find artifact {} in index; reindexing", artifactId);
              artifactIndex.indexArtifact(artifactData);
              break;
            case UNCOMMITTED:
              break;
            case COMMITTED:
              // Requeue the copy of this artifact from temporary to permanent storage
              log.trace("Requeuing pending move to permanent storage for artifact [artifactId: {}]", artifactId);

              try {
                stripedExecutor.submit(new CommitArtifactTask(artifactIndex.getArtifact(artifactId)));
              } catch (RejectedExecutionException e) {
                log.warn(
                    "Could not queue copy of artifact from temporary to permanent " +
                        "storage because executor rejected the task [artifactId: {}]",
                    artifactId
                );
              }

              break;
            case EXPIRED:
            case COPIED:
            case DELETED:
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

      //// Handle this WARC file
      // Q: Is the call to TempWarcInUseTracker still necessary?
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
   * Provides the state of an Artifact in its life cycle.
   * 
   * @param expired
   *          A boolean with the indication of whether the artifact record has
   *          expired.
   * @param artifact
   *          An Artifact with the Artifact.
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
      if (!isTmpStorage(Artifact.getPathFromStorageUrl(artifact.getStorageUrl()))) {

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
   * Determines whether a temporary WARC file is removable.
   *
   * The WARC file is removable if all of the WARC records contained within it may be removed.
   *
   * @param tmpWarc
   * @return
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
   * Determines whether an artifact has been deleted from this data store by examining the repository metadata journal.
   *
   * If an entry for the artifact could not be found, it is assumed deleted.
   *
   * @param aid An {@code ArtifactIdentifier}
   * @return
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

  protected boolean isArtifactCommitted(ArtifactIdentifier aid) throws IOException {
    RepositoryArtifactMetadata metadata = getRepositoryMetadata(aid);

    if (metadata != null) {
      // YES: Repository metadata journal entry found for this artifact
      return metadata.isCommitted();
    }

    return false;
  }

  @Override
  public void shutdownDataStore() throws InterruptedException {
    scheduledExecutor.shutdown();
    stripedExecutor.shutdown();

    // TODO: Parameterize
    scheduledExecutor.awaitTermination(1, TimeUnit.MINUTES);
    stripedExecutor.awaitTermination(1, TimeUnit.MINUTES);

    dataStoreState = DataStoreState.SHUTDOWN;
    log.info("Finished shutdown of data store");
  }

  /**
   * Determines whether a single WARC record is removable.
   *
   * It is removable if it is expired and not committed, or expired and committed but not pending a copy to permanent
   * storage. If unexpired, it is removable if committed and not pending a copy to permanent storage.
   *
   * @param record An {@code ArchiveRecord} representing a WARC record in a temporary WARC file.
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
   *
   * Should only be used in testing.
   *
   * @param artifactIndex The {@code ArtifactIndex} instance to associate with this WARC artifact data store.
   */
  protected void setArtifactIndex(ArtifactIndex artifactIndex) {
    if (artifactIndex == null) {
      throw new IllegalArgumentException("Null artifact index");
    }

    if (this.artifactIndex != null && this.artifactIndex != artifactIndex) {
      throw new IllegalStateException("Artifact index already set");
    }

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

  /**
   * Stores artifact data to this WARC artifact data store by appending to an available temporary WARC from a pool of
   * temporary WARCs. This strategy was chosen to allow multiple threads to add to this artifact data store
   * simultaneously.
   *
   * @param artifactData
   *          An instance of {@code ArtifactData} to store to this artifact data store.
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

    log.debug( "Adding artifact ({}, {}, {}, {}, {})",
        artifactId.getId(),
        artifactId.getCollection(),
        artifactId.getAuid(),
        artifactId.getUri(),
        artifactId.getVersion()
    );

    // Create a DFOS to contain our serialized artifact (we do this to get the number of bytes in the serialization)
    DeferredTempFileOutputStream dfos = new DeferredTempFileOutputStream((int)DEFAULT_DFOS_THRESHOLD, "addArtifactData");

    try {
      // Serialize artifact to WARC record; write out to DFOS
      long recordLength = writeArtifactData(artifactData, dfos);
      dfos.close();

      // Get an available WARC from the temporary WARC pool
      WarcFile tmpWarcFile = tmpWarcPool.findWarcFile(recordLength);
      Path tmpWarcFilePath = tmpWarcFile.getPath();

      if (log.isDebug2Enabled()) {
        log.debug2(
            "tmpWarcFile: [path: {}, isInPool: {}, isInUse: {}]",
            tmpWarcFilePath,
            tmpWarcPool.isInPool(tmpWarcFile),
            tmpWarcPool.isInUse(tmpWarcFile)
        );
      }

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

          // Sanity check on bytes written
          if (bytesWritten != recordLength) {
            log.error(
                "Wrote only {} out of {} bytes! [artifactId: {}, tmpWarcFilePath: {}]",
                bytesWritten,
                recordLength,
                artifactId.getId(),
                tmpWarcFilePath
            );

            // TODO: Rollback

            throw new IOException("Did not write entire WARC record to file");
          }
        }
      } finally {
        // Update temporary WARC stats and return to pool
        tmpWarcFile.setLength(offset + bytesWritten);
        tmpWarcPool.returnWarcFile(tmpWarcFile);
      }

      // Log success
      log.debug2("Wrote {} bytes starting at byte offset {} to {}; size is now {}",
          recordLength,
          offset,
          tmpWarcFilePath,
          offset + recordLength
      );

      // Set artifact data storage URL and repository state
      artifactData.setStorageUrl(makeStorageUrl(tmpWarcFilePath, offset, recordLength));
      artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(artifactId, false, false));

      // Write artifact metadata to journal - TODO: Generalize this to write all of an artifact's metadata
      initWarc(getAuMetadataWarcPath(artifactId, RepositoryArtifactMetadata.LOCKSS_METADATA_ID));
      updateArtifactMetadata(artifactId, artifactData.getRepositoryMetadata());

      // Index the artifact
      artifactIndex.indexArtifact(artifactData);

    } finally {
      // Delete the temporary file if one was created
      dfos.deleteTempFile();
    }

    // Create a new Artifact object to return; should reflect artifact data as it is in the data store
    Artifact artifact = new Artifact(
        artifactId,
        false,
        artifactData.getStorageUrl(),
        artifactData.getContentLength(),
        artifactData.getContentDigest()
    );

    // Save the artifact collection date.
    artifact.setCollectionDate(artifactData.getCollectionDate());

    log.debug("Added artifact {}", artifact);

    return artifact;
  }

  @Override
  public ArtifactData getArtifactData(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Artifact is null");
    }

    String artifactId = artifact.getId();
    String storageUrl = artifact.getStorageUrl();

    log.debug2("Retrieving artifact data [artifactId: {}, storageUrl: {}]", artifactId, storageUrl);

    // Guard against deleted or non-existent artifact
    if (!artifactIndex.artifactExists(artifactId) || isArtifactDeleted(artifact.getIdentifier())) {
      return null;
    }
    
    // Open an InputStream from the WARC file and get the WARC record representing this artifact data
    InputStream warcStream = null;

    try {
      warcStream = getInputStreamFromStorageUrl(storageUrl);
    } catch (Exception e) {
      log.error("Caught exception getting stream from storage URL: ", e);
      log.error("storageUrl = {}", storageUrl);
      log.error("artifact = {}", artifact);
      throw e;
    }

    try {
      Path warcFilePath = Artifact.getPathFromStorageUrl(storageUrl);

      // Check whether the artifact is stored in a temporary WARC file.
      // Yes: Increment the counter of times that the file is in use.
      if (isTmpStorage(warcFilePath)) {
        TempWarcInUseTracker.INSTANCE.markUseStart(warcFilePath);

        // Wrap the stream to be read to allow to register the end of this use of the file. I.e., called when the
        // close() method of the stream is closed.
        warcStream = new CloseCallbackInputStream(
            warcStream,
            closingWarcFilePath -> {
              // Decrement the counter of times that the file is in use.
              TempWarcInUseTracker.INSTANCE.markUseEnd((Path) closingWarcFilePath);
            },
            warcFilePath
        );
      }
    } catch (URISyntaxException use) {
      log.error("Cannot get path from storage URL [storageUrl: {}]", storageUrl, use);
      throw new IllegalArgumentException("Invalid Artifact storage URL [storageUrl: " + storageUrl + "]");
    }

    WARCRecord warcRecord = null;

    try {
      warcRecord = new WARCRecord(warcStream, getClass().getSimpleName() + "#getArtifactData", 0L);
    } catch (Exception e) {
      log.error("Caught exception getting WARC record from storage URL: ", e);
      log.error("storageUrl = {}", storageUrl);
      log.error("artifact = {}", artifact);
      throw e;
    }

    // Convert the WARCRecord object to an ArtifactData
    ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(warcRecord); // FIXME: Move to ArtifactDataUtil or ArtifactData

    // Save the underlying input stream so that it can be closed when needed.
    artifactData.setClosableInputStream(warcStream);

    // Set ArtifactData properties
    artifactData.setIdentifier(artifact.getIdentifier());
    artifactData.setStorageUrl(artifact.getStorageUrl());
    artifactData.setContentLength(artifact.getContentLength());
    artifactData.setContentDigest(artifact.getContentDigest());
    artifactData.setRepositoryMetadata(getRepositoryMetadata(artifact.getIdentifier()));

    // Return an ArtifactData from the WARC record
    return artifactData;
  }

  // TODO: Generalize this to arbitrary metadata
  @Override
  public synchronized RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) throws IOException {
    Objects.requireNonNull(artifactId, "Artifact identifier is null");
    Objects.requireNonNull(artifactMetadata, "Repository artifact metadata is null");

    Path auMetadataWarcPath = getAuMetadataWarcPath(artifactId, RepositoryArtifactMetadata.LOCKSS_METADATA_ID);

    log.debug2("artifactId = {}", artifactId);
    log.debug2("auMetadataWarcPath = {}", auMetadataWarcPath);
    log.debug2("artifactMetadata = {}", artifactMetadata);

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
   * @param artifact
   *          The {@code Artifact} to commit to permanent storage.
   * @return An {@code Future<Artifact>} reflecting the new committed state and storage URL.
   * @throws IOException
   */
  @Override
  public Future<Artifact> commitArtifactData(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Artifact is null");
    }

    log.debug2("Committing artifact {} in AU {}", artifact.getId(), artifact.getAuid());

    // Read current state of this artifact from the repository metadata journal
    RepositoryArtifactMetadata artifactState = getRepositoryMetadata(artifact.getIdentifier());

    try {
      // Determine what action to take based on the state of the artifact
      // Q: Race condition possible here?
      switch (getArtifactState(false, artifactState.isDeleted(), artifact)) {
        case UNCOMMITTED:
          // Record new committed state to artifact data repository metadata journal
          artifact.setCommitted(true);
          artifactState.setCommitted(true);
          updateArtifactMetadata(artifact.getIdentifier(), artifactState);

        case COMMITTED:
          // Submit the task to copy the artifact data from temporary to permanent storage
          return stripedExecutor.submit(new CommitArtifactTask(artifact));

        case COPIED:
          // This artifact is already marked committed and is in permanent storage. Wrap in Future and return it.
          return new CompletedFuture<Artifact>(artifact);

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
   * Implements {@code Future} that is already completed upon instantiation.
   *
   * @param <T> Type of the return object.
   */
  private class CompletedFuture<T> implements Future<T> {
    private T v;

    public CompletedFuture(T v) {
      this.v = v;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return v;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return v;
    }
  }

  /**
   * Implementation of {@code Callable} that commits an artifact from temporary to permanent storage.
   *
   * This is implemented as a {@code StripedCallable} because we maintain one active WARC file per AU in which to commit
   * artifacts permanently.
   */
  private class CommitArtifactTask implements StripedCallable<Artifact> {
    private Artifact artifact;

    /**
     * Constructor of {@code CommitArtifactTask}.
     *
     * @param artifact The {@code Artifact} whose artifact data should be copied from temporary to permanent storage.
     */
    public CommitArtifactTask(Artifact artifact) {
      this.artifact = artifact;

    }

    /**
     * Returns this equivalence class or "stripe" that this task belongs to.
     *
     * @return
     */
    @Override
    public Object getStripe() {
      return artifact.getAuid();
    }

    /**
     * Implementation of Callable call() which does the work of copying an artifact from temporary to permanent storage.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Artifact call() throws Exception {
      // TODO: Futures (resulting from submitting this Callable to an ExecutorService) do not capture exceptions: Any
      //  problems need to be handled here. Requeue tasks to try again later.
      return moveToPermanentStorage(artifact);
    }
  }

  /**
   * Moves the WARC record of an artifact from temporary storage to a WARC in permanent storage, and updates the
   * storage URL if successful.
   *
   * @param artifact The {@code Artifact} whose backing WARC record should be moved from temporary to permanent storage.
   * @return The {@code Artifact} with an updated storage URL.
   * @throws IOException
   */
  protected Artifact moveToPermanentStorage(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Artifact is null");
    }

    WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(artifact.getStorageUrl());

    long recordOffset = loc.getOffset();
    long recordLength = loc.getLength();

    // Get the current active permanent WARC for this AU
    Path dst = getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid());
    initWarc(dst);

    // Artifact will be appended as a WARC record to this WARC file so its offset is the current length of the file
    long warcLength = getWarcLength(dst);

    // Calculate waste space in last block
    long wasteSealing = (getBlockSize() - (warcLength % getBlockSize())) % getBlockSize();
    long wasteAppending = (getBlockSize() - ((warcLength + recordLength) % getBlockSize())) % getBlockSize();

    log.debug2(
        "[warcLength: {}, recordOffset: {}, recordLength: {}, wasteSealing: {}, wasteAppending: {}]",
        warcLength, recordOffset, recordLength, wasteSealing, wasteAppending
    );

    boolean sealBeforeAppend = false;

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

    if (sealBeforeAppend) {
      // Seal active WARC
      sealActiveWarc(artifact.getCollection(), artifact.getAuid());

      // Get path to new active WARC and ensure it exists
      dst = getAuActiveWarcPath(artifact.getCollection(), artifact.getAuid());
      initWarc(dst);
      warcLength = getWarcLength(dst);
    }

    // Append WARC record to active WARC
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
        sealActiveWarc(artifact.getCollection(), artifact.getAuid());
      }
    }

    // Set the artifact's new storage URL and update the index
    artifact.setStorageUrl(makeStorageUrl(dst, warcLength, recordLength));
    artifactIndex.updateStorageUrl(artifact.getId(), artifact.getStorageUrl());

    return artifact;
  }

  /**
   * Computes the bytes used in the last block, assuming all previous blocks are maximally filled.
   *
   * @param size
   * @return
   */
  protected long getBytesUsedLastBlock(long size) {
    return ((size - 1) % getBlockSize()) + 1;
  }

  /**
   * Removes an artifact from this artifact data store.
   *
   * @param artifact The {@code Artifact} whose {@code ArtifactData} will be removed from this artifact store.
   * @return
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
        // Write new state to artifact data repository metadata journal
        repoMetadata.setDeleted(true);
        updateArtifactMetadata(artifact.getIdentifier(), repoMetadata);

        // Update artifact index
        artifactIndex.deleteArtifact(artifact.getId());

        // TODO: Remove artifact from storage - cutting and splicing WARC files is expensive so we just leave the
        //       artifact in place for now.
      } else {
        log.warn("Artifact is already deleted [artifact: {}]", artifact);
      }
    } catch (Exception e) {
      log.error("Caught exception deleting artifact [artifact: {}]", artifact, e);
      throw e;
    }

    log.debug2("Deleted artifact [artifactId: {}]", artifact.getId());
  }

  public DataStoreState getDataStoreState() {
    return dataStoreState;
  }

  public String generateSealedWarcName(String collection, String auid) {
    String auidHash = DigestUtils.md5Hex(auid);
    String timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(FMT_TIMESTAMP);
    return collection + "_" + AU_DIR_PREFIX + auidHash + "_" + timestamp + "_" + AU_ARTIFACTS_WARC_NAME;
  }

  protected String generateActiveWarcName(String collection, String auid) {
    String timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(FMT_TIMESTAMP);
    String auidHash = DigestUtils.md5Hex(auid);
    return String.format("artifacts_%s-%s_%s.warc", collection, auidHash, timestamp);
  }

  /**
   * Seals the active WARC of an AU in permanent storage from further writes.
   *
   * @param collection A {@code String} containing the collection ID of the AU.
   * @param auid A {@code String} containing the AUID of the AU.
   * @throws IOException
   */
  public void sealActiveWarc(String collection, String auid) throws IOException {
    // Key into the active WARC map
    RepoAuid au = new RepoAuid(collection, auid);

    synchronized (auActiveWarcMap) {
      if (auActiveWarcMap.containsKey(au)) {
        // Queue delivery of current active WARC to third-party applications
//        stripedExecutor.submit(new DeliverSealedWarcTask(collection, auid));

        // Clear the active WARC path for this AU
        auActiveWarcMap.remove(au);
      } else {
        log.debug2("AU has no active WARC to seal [collection: {}, auid: {}]", collection, auid);
      }
    }
  }

  /**
   * This implementation of {@code StripedRunnable} wraps {@code sealActiveWarc()} to allow for out-of-band requests to
   * seal the active WARC of an AU are handled in the correct chronological order. This is achieved by using the same
   * stripe used by {@code CommitArtifactTask}.
   */
  private class SealActiveWarcTask implements StripedRunnable {
    private final String collection;
    private final String auid;

    public SealActiveWarcTask(String collection, String auid) {
      this.collection = collection;
      this.auid = auid;
    }

    @Override
    public Object getStripe() {
      return auid;
    }

    @Override
    public void run() {
      try {
        sealActiveWarc(collection, auid);
      } catch (IOException e) {
        log.error("Failed to seal active WARC for [Collection: {}, AUID: {}]: {}", collection, auid, e);
      }
    }
  }

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

  /**
   * Parses a storage URL containing a byte offset
   *
   * @param storageUrl
   * @return
   * @throws IOException
   */
  protected InputStream getInputStreamFromStorageUrl(String storageUrl) throws IOException {
    WarcRecordLocation loc = WarcRecordLocation.fromStorageUrl(storageUrl);
    return getInputStreamAndSeek(loc.getPath(), loc.getOffset());
  }

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

    public static WarcRecordLocation fromStorageUrl(String storageUrl) {
      try {
        URI storageUri = new URI(storageUrl);

        // Get path to WARC file
        Path path = Paths.get(storageUri.getPath());

        // Get WARC record offset and length
        MultiValueMap queryArgs = parseQueryArgs(storageUri.getQuery());
        long offset = Long.parseLong((String) queryArgs.getFirst("offset"));
        long length = Long.parseLong((String) queryArgs.getFirst("length"));

        log.debug2(
            "path = {}, offset = {}, length = {} [storageUrl: {}]",
            path,
            offset,
            length,
            storageUrl
        );

        return new WarcRecordLocation(path, offset, length);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Bad storage URL");
      }
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
    replayRepositoryMetadata(index, repoMetadataWarcFiles);
  }

  public static final String LOCKSS_METADATA_ARTIFACTID_KEY = "artifactId";

  /**
   * Reads an artifact's current repository metadata state from storage.
   *
   * @param aid
   * @return
   * @throws IOException
   */
  protected RepositoryArtifactMetadata getRepositoryMetadata(ArtifactIdentifier aid) throws IOException {
    Map<String, JSONObject> journal =
        readMetadataJournal(getAuMetadataWarcPath(aid, RepositoryArtifactMetadata.LOCKSS_METADATA_ID));

    JSONObject json = journal.get(aid.getId());

    if (json != null) {
      return new RepositoryArtifactMetadata(journal.get(aid.getId()));
    }

    log.warn("Could not find any journal entries for artifact [artifactId: {}]", aid.getId());
    return null;
  }

  /**
   * Reads the journal for a class of artifact metadata from a WARC file at a given path, and builds a map from artifact
   * ID to its most recently journaled metadata.
   *
   * @param metadataFile A {@code String} containing the path to the WARC file containing artifact metadata.
   * @return A {@code Map<String, JSONObject>} mapping artifact ID to its latest metadata.
   * @throws IOException
   *
   * TODO: This is slow and inefficient as the number of stale entries in the journal grows. Some external process
   *       should periodically prune the journal. Additionally, some sort of caching could be beneficial since this is
   *       called often.
   */
  private Map<String, JSONObject> readMetadataJournal(Path metadataFile) throws IOException {
    Map<String, JSONObject> metadata = new HashMap<>();

    try (InputStream warcStream = markAndGetInputStream(metadataFile)) {

      for (ArchiveRecord record : new UncompressedWARCReader("WarcArtifactDataStore", warcStream)) {
        // Determine whether this is a WARC metadata type record
        boolean isWarcMetadataRecord = ((String) record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE))
            .equalsIgnoreCase(WARCRecordType.metadata.toString());

        if (isWarcMetadataRecord) {
          JSONObject json = new JSONObject(IOUtils.toString(record, DEFAULT_ENCODING));
          String artifactId = json.getString(LOCKSS_METADATA_ARTIFACTID_KEY);
          metadata.put(artifactId, json);
        }
      }

      return metadata;
    }
  }

  private void replayRepositoryMetadata(ArtifactIndex index, Collection<Path> metadataFiles) throws IOException {
    for (Path metadataFile : metadataFiles) {
      replayRepositoryMetadata(index, metadataFile);
    }
  }

  /**
   * Reads and replays repository metadata to a given artifact index.
   *
   * @param index An {@code ArtifactIndex} to replay repository metadata to.
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

  private InputStream markAndGetInputStream(Path warcFile) throws IOException {
    return markAndGetInputStreamAndSeek(warcFile, 0L);
  }

  private InputStream markAndGetInputStreamAndSeek(Path warcFile, long offset) throws IOException {
    TempWarcInUseTracker.INSTANCE.markUseStart(warcFile);

    InputStream warcStream = new BufferedInputStream(getInputStreamAndSeek(warcFile, offset));

    return new CloseCallbackInputStream(
        warcStream,
        closingWarcFile -> {
          // Decrement the counter of times that the file is in use.
          TempWarcInUseTracker.INSTANCE.markUseEnd((Path) closingWarcFile);
        },
        warcFile
    );
  }

  /**
   * For debugging. Dumps the bytes of a WARC file to log.
   *
   * @param path A {@code String} containing the path to a WARC file.
   * @throws IOException
   */
  private void dumpWarc(Path path) throws IOException {
    if (log.isDebug2Enabled()) {
      InputStream is = getInputStream(path);
      org.apache.commons.io.output.ByteArrayOutputStream baos = new ByteArrayOutputStream();

      IOUtils.copy(is, baos);

      log.debug2("dumpWarc({}):\n{}", path, baos.toString());
    }
  }

  private void reindexArtifactsFromWarcs(ArtifactIndex index, Collection<Path> artifactWarcFiles) throws IOException {
    // Reindex artifacts from temporary and permanent storage
    for (Path warcFile : artifactWarcFiles) {

      // Debugging
      //dumpWarc(warcFile);

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
              artifactData.setStorageUrl(makeStorageUrl(warcFile, record.getHeader().getOffset(), recordLength));

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

  private class UncompressedWARCReader extends WARCReader {
    public UncompressedWARCReader(final String f, final InputStream is) {
      setIn(new CountingInputStream(is));
      initialize(f);
    }

    @Override
    protected WARCRecord createArchiveRecord(InputStream is, long offset) throws IOException {
      return (WARCRecord)currentRecord(new WARCRecord(new SimpleRepositionableStream(is), getReaderIdentifier(), offset, isDigest(), isStrict()));
    }
  }

  // *******************************************************************************************************************
  // * WARC
  // *******************************************************************************************************************

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
//        sb.append(HEADER_KEY_ID).append(COLON_SPACE).append(record.getRecordId().toString()).append(CRLF);
    sb.append(CONTENT_LENGTH).append(COLON_SPACE).append(Long.toString(record.getContentLength())).append(CRLF);
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
//            record.getExtraHeaders().stream().map(x -> sb.append(x).append(CRLF));

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
   * @param extraHeaders
   * @return
   */
  public static WARCRecordInfo createWARCInfoRecord(MultiValueMap<String, String> extraHeaders) {
    return createWARCInfoRecord(extraHeaders, null, null);
  }

  public static WARCRecordInfo createWARCInfoRecord(MultiValueMap<String, String> headers, MediaType mimeType, byte[] content) {
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

  // *******************************************************************************************************************
  // * JMS
  // *******************************************************************************************************************

  /**
   * Returns this WARC artifact data store's JMS consumer handle.
   *
   * @return The {@code JmsConsumer} associated with this WARC artifact data store.
   */
  public JmsConsumer getJmsConsumer() {
    return jmsConsumer;
  }

  /**
   * Creates the JMS consumer.
   */
  private void makeJmsConsumer() {
    new Thread(new Runnable() {
	@Override
	public void run() {
	  log.info("Creating JMS consumer");
	  while (jmsConsumer == null) {
	    // In Spring env, lockssRepo doesn't get set until some time
	    // after this starts running
	    if (lockssRepo != null && lockssRepo instanceof JmsFactorySource) {
	      JmsFactory fact = ((JmsFactorySource)lockssRepo).getJmsFactory();
	      try {
		log.trace("Attempting to create JMS consumer");
		jmsConsumer = fact.createTopicConsumer(CLIENT_ID, JMS_TOPIC, new DataStoreCrawlListener("AuEvent Listener"));
		log.info("Successfully created JMS consumer");
		break;
	      } catch (JMSException | NullPointerException exc) {
		log.trace("Could not establish JMS connection; sleeping and retrying");
	      }
	    }
	    TimerUtil.guaranteedSleep(retryDelay);
	  }
	}
      }).start();
  }

  public static final String KEY_AUID = "auid";
  public static final String KEY_TYPE = "type";
  public static final String CONTENT_CHANGED = "ContentChanged";
  public static final String KEY_REPO_SPEC = "repository_spec";
  static Pattern REPO_SPEC_PATTERN =
      Pattern.compile("([^:]+):([^:]+)(?::(.*$))?");

  private class DataStoreCrawlListener implements MessageListener {

    DataStoreCrawlListener(String listenerName) {
    }

    @Override
    public void onMessage(Message message) {

      try {
        Map<String, Object> msgMap = (Map<String, Object>) JmsUtil.convertMessage(message);
        String auId = (String) msgMap.get(KEY_AUID);
        String event = (String) msgMap.get(KEY_TYPE);
        String repospec = (String) msgMap.get(KEY_REPO_SPEC);
        log.debug2("Received notification: [AuEvent: {} AU: {} change: {}]", event, auId, repospec);
        if (auId != null && repospec != null && CONTENT_CHANGED.equals(event)) {
          Matcher m1 = REPO_SPEC_PATTERN.matcher(repospec);
          if (m1.matches()) {
            String collection = m1.group(2);
            if (collection != null) {
              stripedExecutor.submit(new SealActiveWarcTask(collection, auId));
            }
          }
        }
      } catch (JMSException e) {
        log.error("Unable to understand crawl listener jms message.");
      }
    }
  }

}
