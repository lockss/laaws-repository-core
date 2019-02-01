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

package org.lockss.laaws.rs.io.storage.warc;

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.*;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.util.anvl.Element;
import org.lockss.laaws.rs.io.RepoAuid;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.lockss.laaws.rs.util.JmsConsumer;
import org.lockss.log.L4JLogger;
import org.lockss.util.concurrent.stripedexecutor.StripedCallable;
import org.lockss.util.concurrent.stripedexecutor.StripedExecutorService;
import org.lockss.util.concurrent.stripedexecutor.StripedRunnable;
import org.lockss.util.time.TimeUtil;
import org.lockss.util.time.TimerUtil;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.*;
import java.net.URI;
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

  protected static final String WARC_FILE_EXTENSION = ".warc";
  protected static final String COLLECTIONS_DIR = "collections";
  protected static final String TMP_WARCS_DIR = "tmp";
  protected static final String SEALED_WARC_DIR = "sealed";
  protected static final String AU_DIR_PREFIX = "au-";
  protected static final String AU_ARTIFACTS_WARC_NAME = "artifacts" + WARC_FILE_EXTENSION;

  protected static final String SCHEME = "urn:uuid";
  protected static final String CRLF = "\r\n";
  protected static byte[] CRLF_BYTES;
  protected static String SEPARATOR = "/";

  private static final long DEFAULT_DFOS_THRESHOLD = 16L * FileUtils.ONE_MB;

  protected static final String ENV_THRESHOLD_WARC_SIZE = "REPO_MAX_WARC_SIZE";
  protected static final long DEFAULT_THRESHOLD_WARC_SIZE = 100L * FileUtils.ONE_MB;
  protected long thresholdWarcSize;

  protected static final String ENV_UNCOMMITTED_ARTIFACT_EXPIRATION = "REPO_UNCOMMITTED_ARTIFACT_EXPIRATION";
  protected static final long DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION = TimeUtil.WEEK;
  protected long uncommittedArtifactExpiration;

  protected ArtifactIndex artifactIndex;
  protected String basePath;
  protected WarcFilePool tmpWarcPool;
  protected StripedExecutorService execsvc;
  protected Map<RepoAuid, String> auActiveWarcMap;

  protected Pattern fileAndOffsetStorageUrlPat;

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
  protected abstract String getTmpWarcBasePath();
  protected abstract String makeStorageUrl(String filePath, MultiValueMap<String, String> params);

  protected abstract InputStream getInputStream(String filePath) throws IOException;
  protected abstract InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException;
  protected abstract InputStream getWarcRecordInputStream(String storageUrl) throws IOException;
  protected abstract OutputStream getAppendableOutputStream(String filePath) throws IOException;

  protected abstract void initWarc(String warcPath) throws IOException;
  protected abstract void moveWarc(String srcPath, String dstPath) throws IOException;
  protected abstract Collection<String> findWarcs(String basePath) throws IOException;
  protected abstract boolean removeWarc(String warcPath) throws IOException;

  protected abstract long getFileLength(String filePath) throws IOException;

  protected abstract long getBlockSize();

  // *******************************************************************************************************************
  // * CONSTRUCTORS
  // *******************************************************************************************************************

  /**
   * Constructor for a WARC artifact data store.
   *
   */
  public WarcArtifactDataStore() {
    this.execsvc = new StripedExecutorService();
    this.auActiveWarcMap = new HashMap<>();

    setThresholdWarcSize(NumberUtils.toLong(System.getenv(ENV_THRESHOLD_WARC_SIZE), DEFAULT_THRESHOLD_WARC_SIZE));

    setUncommittedArtifactExpiration(NumberUtils.toLong(
        System.getenv(ENV_UNCOMMITTED_ARTIFACT_EXPIRATION),
        DEFAULT_UNCOMMITTED_ARTIFACT_EXPIRATION
    ));

    makeJmsConsumer();
  }

  /**
   * Constructor of this WARC artifact data store.
   *
   * @param basePath A {@code String} containing the base path of this WARC artifact data store.
   */
  public WarcArtifactDataStore(String basePath) {
    this();

    this.basePath = basePath;
    this.tmpWarcPool = new WarcFilePool(getTmpWarcBasePath());
  }

  // *******************************************************************************************************************
  // * METHODS
  // *******************************************************************************************************************

  public String makeStorageUrl(String filePath, long offset, long length) {
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    params.add("offset", Long.toString(offset));
    params.add("length", Long.toString(length));
    return makeStorageUrl(filePath, params);
  }

  /**
   * Reads and reloads state from temporary WARCs, including the requeuing of copy tasks of committed artifacts from
   * temporary to permanent storage. Removes temporary WARCs if eligible:
   *
   * A temporary WARC may be removed if the records contained within it are the serializations of artifacts that are
   * either uncommitted-but-expired or committed-and-moved-to-permanent-storage.
   */
  protected void reloadTmpWarcs() throws IOException {
    if (Objects.isNull(artifactIndex)) {
      throw new IllegalStateException("Cannot reload temporary WARCs without an artifact index");
    }

    String tmpWarcBasePath = new File(getBasePath(), getTmpWarcBasePath()).getPath();
    log.info("Reloading temporary WARCs from {}", tmpWarcBasePath);

    Collection<String> tmpWarcs = findWarcs(tmpWarcBasePath);
    log.info("Found {} temporary WARCs: {}", tmpWarcs.size(), tmpWarcs);

    for (String tmpWarc : tmpWarcs) {
      // True unless the WARC contains a record that cannot be removed (or if there's an error)
      boolean isTmpWarcRemoveable = true;

      // Read from temporary WARC
      try (BufferedInputStream bufferedStream = new BufferedInputStream(getInputStream(tmpWarc))) {

        ArchiveReader archiveReader = WARCReaderFactory.get(tmpWarc, bufferedStream, true);

        // Handle WARC records
        for (ArchiveRecord record : archiveReader) {
          boolean recordRemoveable = isTempWarcRecordRemovable(record);
          isTmpWarcRemoveable &= recordRemoveable;

          if (!recordRemoveable) {
            // WARC record not removable
            ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

            // Get artifact from index to determine committed status and current storage URL
            Artifact artifact = artifactIndex.getArtifact(artifactData.getIdentifier().getId());
            String storageUrl = artifact.getStorageUrl();

            // Requeue if committed but storage URL points to temporary WARC
            if (artifact.getCommitted() && storageUrl.equals(tmpWarc)) {
              execsvc.submit(new CommitArtifactTask(artifact));
            }

            log.warn("*** Reload temporary WARC: {}", record);
          }
        }

        // Handle this WARC file
        if (isTmpWarcRemoveable) {
          bufferedStream.close();
          removeWarc(tmpWarc);
        } else {
          // Return WARC to temporary WARC pool
          long tmpWarcFileLen = getFileLength(tmpWarc);
          tmpWarcPool.returnWarcFile(new WarcFile(tmpWarc, tmpWarcFileLen));
        }

      } catch (IOException e) {
        log.error("Caught IOException while trying to reload temporary WARC: {}: {}", tmpWarc, e);
      }
    }
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
    String recordId = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_ID);
    String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

    // WARC records not of type "response" or "resource" are okay to remove
    // Q: Is this true? An idea for implementing artifact metadata is as WARC metadata records.
    if (!recordType.equalsIgnoreCase(WARCRecordType.response.toString()) &&
        !recordType.equalsIgnoreCase(WARCRecordType.resource.toString())) {
        return true;
    }

    // Parse WARC-Date field and determine if this record / artifact is expired (same date should be in index)
    String warcDate = headers.getDate();
    Instant created = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(warcDate));
    Instant expires = created.plus(getUncommittedArtifactExpiration(), ChronoUnit.MILLIS);
    boolean expired = expires.isBefore(Instant.now());

    try {
      // Get artifact state from index
      Artifact artifact = artifactIndex.getArtifact(artifactId);

      if (artifact != null) {
        // YES: Artifact exists in index
        boolean committed = artifact.getCommitted();

        if (!expired && !committed) {
          // Unexpired and uncommitted -> Keep this temp WARC record
          return false;
        } else {
          // Expired and committed     -> Remove temp record (if successfully committed) + keep in index
          // Unexpired and committed   -> Remove temp record (if successfully committed) + keep in index

          if (committed) {
            // Q: Should we verify the commit was successful? Do we trust the committed bit from the index?
          } else if (expired) {
            // Expired and uncommitted   -> Remove temp record + remove from index
            // TODO: Remove artifact from index
          }

          // Remove temp this WARC record
          return true;
        }
      } else {
        // NO: Artifact does not exists in index

        if (!expired) {
          // TODO: Attempt to index as an uncommitted artifact

          // Keep this temp WARC record
          return false;
        }
      }

      // Q: Should we verify committed and deleted status recorded in the repository metadata journal?
      /*
      // Read the repository metadata journal that should contain entries about this artifact
      Map<String, RepositoryArtifactMetadata> repoMetadata = readRepositoryMetadata("some path");
      RepositoryArtifactMetadata artifactRepoMetadata = repoMetadata.get(artifact.getId());

      boolean committed = artifactRepoMetadata.isCommitted();
      boolean deleted = artifactRepoMetadata.isDeleted();
      */
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
   * Returns the base path of this LOCKSS repository.
   *
   * @return A {@code String} containing the base path of this LOCKSS repository.
   */
  public String getBasePath() {
    return basePath;
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
   * Depreciated. Using this may have unpredictable results.
   *
   * @param artifactIndex The {@code ArtifactIndex} instance to associate with this WARC artifact data store.
   */
  protected void setArtifactIndex(ArtifactIndex artifactIndex) {
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

    log.info(String.format(
        "Adding artifact (%s, %s, %s, %s, %s)",
        artifactId.getId(),
        artifactId.getCollection(),
        artifactId.getAuid(),
        artifactId.getUri(),
        artifactId.getVersion()
    ));

    // Create a DFOS to contain our serialized artifact
    try (DeferredFileOutputStream dfos = new DeferredFileOutputStream(
        (int)DEFAULT_DFOS_THRESHOLD,
        "addArtifactData",
        null,
        new File("/tmp") // FIXME: Refactor repository code to use a custom temp dir
    )) {
      // Serialize artifact to WARC record and get the number of bytes in the serialization
      long bytesWritten = writeArtifactData(artifactData, dfos);
      dfos.close();

      // Get an available temporary WARC from the temporary WARC pool
      WarcFile tmpWarcFile = tmpWarcPool.findWarcFile(bytesWritten);
      String tmpWarcFilePath = tmpWarcFile.getPath();

      // Initialize the WARC
      initWarc(tmpWarcFilePath);

      // The offset for the record to be appended to this WARC is the length of the WARC file (i.e., its end)
      long offset = getFileLength(tmpWarcFilePath);

      // Write serialized artifact to temporary WARC file
      try (OutputStream output = getAppendableOutputStream(tmpWarcFilePath)) {
        dfos.writeTo(output);

        log.info(String.format(
            "Wrote %d bytes starting at byte offset %d to %s; size is now %d",
            bytesWritten,
            offset,
            tmpWarcFilePath,
            offset + bytesWritten
        ));
      }

      // Update temporary WARC stats and return to pool
      tmpWarcFile.setLength(offset + bytesWritten);
      tmpWarcPool.returnWarcFile(tmpWarcFile);

      // Set artifact data storage URL
      artifactData.setStorageUrl(makeStorageUrl(tmpWarcFilePath, offset, bytesWritten));
    }

    // Write artifact data metadata - TODO: Generalize this to write all of an artifact's metadata
    artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(artifactId, false, false));
    initWarc(getAuMetadataWarcPath(artifactId, artifactData.getRepositoryMetadata()));
    updateArtifactMetadata(artifactId, artifactData.getRepositoryMetadata());

    // Create a new Artifact object to return; should reflect artifact data as it is in the data store
    Artifact artifact = new Artifact(
        artifactId,
        false,
        artifactData.getStorageUrl(),
        artifactData.getContentLength(),
        artifactData.getContentDigest()
    );

    return artifact;
  }

  @Override
  public ArtifactData getArtifactData(Artifact artifact) throws IOException {
    Objects.requireNonNull(artifact, "Artifact is null");

    log.info(String.format("Retrieving artifact data for artifact ID: %s", artifact.getId()));

    // Open an InputStream from the WARC file and get the WARC record representing this artifact data
    InputStream warcStream = getWarcRecordInputStream(artifact.getStorageUrl());
    WARCRecord warcRecord = new WARCRecord(warcStream, getClass().getSimpleName() + "#getArtifactData", 0L);

    // Convert the WARCRecord object to an ArtifactData
    ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(warcRecord); // FIXME: Move to ArtifactDataUtil or ArtifactData

    // Save the underlying input stream so that it can be closed when needed.
    artifactData.setClosableInputStream(warcStream);

    // Set ArtifactData properties
    artifactData.setIdentifier(artifact.getIdentifier());
    artifactData.setStorageUrl(artifact.getStorageUrl());
    artifactData.setContentLength(artifact.getContentLength());
    artifactData.setContentDigest(artifact.getContentDigest());
    artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(
        artifact.getIdentifier(),
        artifact.getCommitted(),
        false
    ));

    // Return an ArtifactData from the WARC record
    return artifactData;
  }

  // TODO: Generalize this to arbitrary metadata
  @Override
  public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) throws IOException {
    Objects.requireNonNull(artifactId, "Artifact identifier is null");
    Objects.requireNonNull(artifactMetadata, "Repository artifact metadata is null");

    String auMetadataWarcPath = getAuMetadataWarcPath(artifactId, artifactMetadata);

    // Initialize metadata WARC file
    initWarc(auMetadataWarcPath);

    try (
        OutputStream out = getAppendableOutputStream(auMetadataWarcPath)
    ) {
      // Append WARC metadata record to the journal
      WARCRecordInfo metadataRecord = createWarcMetadataRecord(artifactId.getId(), artifactMetadata);
      writeWarcRecord(metadataRecord, out);
      out.flush();
    }

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
    Objects.requireNonNull(artifact, "Artifact is null");

    if (artifactIndex == null) {
      throw new IllegalStateException("Artifact index is null");
    }

    log.info("Committing {} of AUID {}", artifact.getId(), artifact.getAuid());

    // Read current state of repository metadata for this artifact
    ArtifactData artifactData = getArtifactData(artifact);
    RepositoryArtifactMetadata artifactState = artifactData.getRepositoryMetadata();

    // Proceed only if the artifact is not marked deleted
    // TODO: This is always false - see getArtifactData() implementation
    if (artifactState.isDeleted()) {
      log.warn("Artifact is deleted (Artifact ID: {})", artifact.getId());
      return null;
    }

    // Write new committed state to artifact data repository metadata journal
    artifact.setCommitted(true);
    artifactState.setCommitted(true);
    updateArtifactMetadata(artifact.getIdentifier(), artifactState);

    // Mark the artifact as committed in the index
    log.info("artifactIndex = {}", artifactIndex);
    log.info("artifact = {}", artifact);
    artifactIndex.commitArtifact(artifact.getId());

    // Submit the task to copy the artifact data from temporary to permanent storage
    Future<Artifact> artifactFuture = execsvc.submit(new CommitArtifactTask(artifact));

    return artifactFuture;
  }

  /**
   * Implementation of {@code Callable} that commits an artifact from temporary to permanent storage.
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
  // FIXME: Stream WARC record instead of unserializing only to serialize again
  protected Artifact moveToPermanentStorage(Artifact artifact) throws IOException {
    // Read artifact data from current WARC file
    ArtifactData artifactData = getArtifactData(artifact);

    // Create a DFOS to contain our serialized artifact
    DeferredFileOutputStream dfos = new DeferredFileOutputStream(
        (int)DEFAULT_DFOS_THRESHOLD,
        "moveToPermanentStorage",
        null,
        new File("/tmp") // FIXME: Refactor to use custom temporary directory
    );

    // Serialize artifact to WARC record and get the number of bytes in the serialization
    long recordLength = writeArtifactData(artifactData, dfos);
    dfos.close();

    // Get the current active permanent WARC for this AU
    String dst = getActiveWarcPath(artifact.getCollection(), artifact.getAuid());
    initWarc(dst);

    // Artifact will be appended as a WARC record to this WARC file so its offset is the current length of the file
    long warcLength = getFileLength(dst);

    // Calculate waste space in last block
    long wasteSealing = (getBlockSize() - (warcLength % getBlockSize())) % getBlockSize();
    long wasteAppending = (getBlockSize() - ((warcLength + recordLength) % getBlockSize())) % getBlockSize();

    boolean sealBeforeAppend = false;

    if (warcLength < getThresholdWarcSize()) {
      if (warcLength + recordLength <= getThresholdWarcSize()) {
        // Append
//        sealBeforeAppend = false;
      } else {
        if (wasteSealing > wasteAppending) {
          // Append
//          sealBeforeAppend = false;
        } else {
          // Seal then append
          sealBeforeAppend = true;
        }
      }
    } else {
      if (recordLength <= wasteSealing) {
        // Append
//        sealBeforeAppend = false;
      } else {
        // Seal then append
        sealBeforeAppend = true;
      }
    }

    if (sealBeforeAppend) {
      // Seal active WARC and get path to new active WARC
      sealActiveWarc(artifact.getCollection(), artifact.getAuid());
      dst = getActiveWarcPath(artifact.getCollection(), artifact.getAuid());
      warcLength = getFileLength(dst);
    }

    // Append WARC record to active WARC
    try (OutputStream output = getAppendableOutputStream(dst)) {
      // Write serialized artifact to permanent WARC file
      dfos.writeTo(output);

      log.info(String.format(
          "Committed: %s: Wrote %d bytes starting at byte offset %d to %s; size is now %d",
          artifact.getIdentifier().getId(),
          recordLength,
          warcLength,
          dst,
          warcLength + recordLength
      ));
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
  public RepositoryArtifactMetadata deleteArtifactData(Artifact artifact) throws IOException {
    Objects.requireNonNull(artifact, "Artifact is null");

    // Retrieve artifact data from current WARC file
    ArtifactData artifactData = getArtifactData(artifact);
    RepositoryArtifactMetadata repoMetadata = artifactData.getRepositoryMetadata();

    if (!repoMetadata.isDeleted()) {
      // Write new state to artifact data repository metadata journal
      repoMetadata.setCommitted(false);
      repoMetadata.setDeleted(true);
      updateArtifactMetadata(artifact.getIdentifier(), repoMetadata);

      // Update artifact index
//      artifactIndex.deleteArtifact(artifact.getId());

      // TODO: Actually remove artifact from storage. A more likely design is a garbage collector with customizable
      // policy because it is expensive to cut and splice artifacts.
    } else {
      log.warn(String.format("Artifact is already deleted (Artifact ID: %s)", artifact.getId()));
    }

    return repoMetadata;
  }

  public String getCollectionPath(String collection) {
    return SEPARATOR + COLLECTIONS_DIR + SEPARATOR + collection;
  }

  public String getCollectionPath(ArtifactIdentifier artifactIdent) {
    return getCollectionPath(artifactIdent.getCollection());
  }

  public String getCollectionTmpPath(String collection) {
    return getCollectionPath(collection) + SEPARATOR + TMP_WARCS_DIR;
  }

  public String getAuPath(String collection, String auid) {
    return getCollectionPath(collection) + SEPARATOR + AU_DIR_PREFIX + DigestUtils.md5Hex(auid);
  }

  public String getAuPath(ArtifactIdentifier artifactIdent) {
    return getAuPath(artifactIdent.getCollection(), artifactIdent.getAuid());
  }

  public String getSealedWarcsPath() {
    return SEPARATOR + SEALED_WARC_DIR;
  }

  public String generateSealedWarcName(String collection, String auid) {
    String auidHash = DigestUtils.md5Hex(auid);
    String timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(FMT_TIMESTAMP);
    return collection + "_" + AU_DIR_PREFIX + auidHash + "_" + timestamp + "_" + AU_ARTIFACTS_WARC_NAME;
  }

  /**
   * Returns the path to the active WARC of an AU, in permanent storage.
   *
   * @param collection
   * @param auid
   * @return
   */
  public String getActiveWarcPath(String collection, String auid) {
    // TODO: Use Path instead of String concatenation for this type of pattern?
    return getAuPath(collection, auid) + SEPARATOR + getActiveWarcName(collection, auid);
  }

  public String getActiveWarcPath(ArtifactIdentifier artifactIdent) {
    return getActiveWarcPath(artifactIdent.getCollection(), artifactIdent.getAuid());
  }

  public String getActiveWarcName(String collection, String auid) {
    synchronized (auActiveWarcMap) {
      RepoAuid repoAuid = new RepoAuid(collection, auid);
      auActiveWarcMap.putIfAbsent(repoAuid, generateActiveWarcName(collection, auid));
      return auActiveWarcMap.get(repoAuid);
    }
  }

  protected String generateActiveWarcName(String collection, String auid) {
    String timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(FMT_TIMESTAMP);
    return String.format("artifacts_%s-%s_%s.warc", collection, auid, timestamp);
  }

  /**
   * Seals the active WARC being maintained in permanent storage for an AU from further writes, and starts a new active
   * WARC for the AU.
   *
   * @param auid
   */
  public void sealActiveWarc(String collection, String auid) throws IOException {
    // Key into the active WARC map
    RepoAuid au = new RepoAuid(collection, auid);

    // Prevent a commit thread (via getActiveWarcPath(...)) from getting the active WARC and writing to it further
    synchronized (auActiveWarcMap) {
      // Queue delivery of current active WARC to third-party applications
      execsvc.submit(new DeliverSealedWarcTask(collection, auid));

      // Set new active WARC
      auActiveWarcMap.put(au, generateActiveWarcName(collection, auid));

      // Initialize the active WARC file
      initWarc(getActiveWarcPath(collection, auid));
    }
  }

  /**
   * This implementation of {@code StripedRunnable} wraps {@codesealActiveWarc()} to allow for out-of-band requests to
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
   * This implementation of {@code Runnable} encapsulates the steps to deliver a sealed WARC from permanent storage to
   * an external storage location for consumption by third-party services.
   *
   * It is NOT implemented as a {@code StripedRunnable} to allow for the simultaneous delivery of multiple sealed WARCs.
   */
  private class DeliverSealedWarcTask implements Runnable {
    private final String src;
    private final String dst;

    public DeliverSealedWarcTask(String collection, String auid) {
      // These must be bound at construction time
      this.src = getActiveWarcPath(collection, auid);
      this.dst = getSealedWarcsPath() + SEPARATOR + generateSealedWarcName(collection, auid);
    }

    @Override
    public void run() {
      try {
        // Initialize destination WARC
        initWarc(dst);
      } catch (IOException e) {
        log.error("Could not initialize sealed WARC {}: {}", src, e);
      }

      // Stream bytes from InputStream to OutputStream
      try (InputStream in = getInputStream(src)) {
        try (OutputStream out = getAppendableOutputStream(dst)) {
          IOUtils.copy(in, out);
          log.info("Delivered sealed WARC {} to {}", src, dst);
        }
      } catch (IOException e) {
        log.error("Could not deliver sealed WARC {} to {}: {}", src, dst, e);
        // TODO: Requeue task for later retry
      }
    }
  }

  public String getAuMetadataWarcPath(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifactMetadata) {
    return getAuPath(artifactId) + SEPARATOR + artifactMetadata.getMetadataId() + WARC_FILE_EXTENSION;
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
    DeferredFileOutputStream dfos = new DeferredFileOutputStream(16384, "artifactData", null, new File("/tmp"));

    // Create a HTTP response stream from the ArtifactData
    InputStream httpResponse = ArtifactDataUtil.getHttpResponseStreamFromHttpResponse(
        ArtifactDataUtil.getHttpResponseFromArtifactData(artifactData)
    );

    IOUtils.copy(httpResponse, dfos);

    dfos.flush();
    dfos.close();

    // Get the temporary file created, if it exists, so that we can delete
    // it after it's no longer needed.
    File dfosFile = dfos.getFile();

    // Set the length of the artifact data
    long contentLength = artifactData.getBytesRead();
    if (log.isDebugEnabled()) log.debug("contentLength = " + contentLength);
    artifactData.setContentLength(contentLength);
    record.addExtraHeader(ArtifactConstants.ARTIFACT_LENGTH_KEY,
        String.valueOf(contentLength));

    // Set content digest of artifact data
    String contentDigest = String.format("%s:%s",
        artifactData.getMessageDigest().getAlgorithm(),
        new String(Hex.encodeHex(artifactData.getMessageDigest().digest())));
    if (log.isDebugEnabled()) log.debug("contentDigest = " + contentDigest);
    artifactData.setContentDigest(contentDigest);
    record.addExtraHeader(ArtifactConstants.ARTIFACT_DIGEST_KEY,
        contentDigest);

    // Attach WARC record payload and set the payload length
    record.setContentStream(dfos.isInMemory() ? new ByteArrayInputStream(dfos.getData()) : new FileInputStream(dfosFile));
    record.setContentLength(dfos.getByteCount());

    // Write WARCRecordInfo to OutputStream
    CountingOutputStream cout = new CountingOutputStream(outputStream);
    writeWarcRecord(record, cout);

    // Delete the temporary file created, if it exists.
    if (dfosFile != null && !dfosFile.delete()) {
      log.warn("Removal of temporary file " + dfosFile + " failed.");
    }

    return cout.getCount();
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

    // Write the header-body separator
    out.write(CRLF_BYTES);

    if (record.getContentStream() != null) {
      // Write the WARC payload
      int bytesWritten = IOUtils.copy(record.getContentStream(), out);

      // Sanity check
      if (bytesWritten != record.getContentLength()) {
        log.warn(String.format("Expected to write %d bytes, but wrote %d", record.getContentLength(), bytesWritten));
      }
    }

    // Write the two blank lines required at end of every record
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
    return String.format("<%s:%s>", SCHEME, id);
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

  public String makeStorageUrl(String filePath) {
    MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
    return makeStorageUrl(filePath, params);
  }

  /**
   * Parses a storage URL containing a byte offset
   *
   * @param storageUrl
   * @return
   * @throws IOException
   */
  protected InputStream getFileAndOffsetWarcRecordInputStream(String storageUrl) throws IOException {
    Matcher mat = fileAndOffsetStorageUrlPat.matcher(storageUrl);

    if (!mat.matches()) {
      // Shouldn't happen because all these artifacts are in existing directories
      log.error("storageUrl = {}", storageUrl);
      throw new IllegalArgumentException("Bad storage URL");
    }

    return getInputStreamAndSeek(mat.group(3), Long.parseLong(mat.group(4)));
  }

  /**
   * Rebuilds the internal index from WARCs within this WARC artifact data store.
   *
   * @throws IOException
   */
  public void rebuildIndex() throws IOException {
    if (artifactIndex == null) {
      throw new IllegalStateException("No artifact index set");
    }

    rebuildIndex(artifactIndex);
  }

  /**
   * Rebuilds the provided index from WARCs within this WARC artifact data store.
   *
   * @param index The {@code ArtifactIndex} to rebuild and populate from WARCs within this WARC artifact data store.
   * @throws IOException
   */
  public void rebuildIndex(ArtifactIndex index) throws IOException {
    rebuildIndex(index, getBasePath());
  }

  /**
   * Rebuilds the provided index from WARCs within this WARC artifact data store.
   *
   * @param basePath A {@code String} containing the base path of this WARC artifact data store.
   * @throws IOException
   */
  public void rebuildIndex(ArtifactIndex index, String basePath) throws IOException {
    Collection<String> warcPaths = findWarcs(basePath);

    // Build a collection of paths to WARCs containing artifact data
    Collection<String> artifactWarcFiles = warcPaths
        .stream()
        .filter(file -> !file.endsWith("lockss-repo" + WARC_FILE_EXTENSION))
        .collect(Collectors.toList());

    // Reindex artifacts from temporary and permanent storage
    for (String warcFile : artifactWarcFiles) {
      try {
        BufferedInputStream bufferedStream = new BufferedInputStream(getInputStream(warcFile));
        for (ArchiveRecord record : WARCReaderFactory.get("WarcArtifactDataStore", bufferedStream, true)) {
          log.info(String.format(
              "Re-indexing artifact from WARC %s record %s from %s",
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
              warcFile
          ));

          try {
            ArtifactData artifactData = ArtifactDataFactory.fromArchiveRecord(record);

            if (artifactData != null) {
              // Set ArtifactData storage URL
              artifactData.setStorageUrl(makeStorageUrl(warcFile, record.getHeader().getOffset()));

              // Default repository metadata for all ArtifactData objects to be indexed
              artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(
                  artifactData.getIdentifier(),
                  false,
                  false
              ));

              // Add artifact to the index
              index.indexArtifact(artifactData);
            }
          } catch (IOException e) {
            log.error(String.format(
                "IOException caught while attempting to re-index WARC record %s from %s",
                record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                warcFile
            ));

            throw e;
          }

        }
      } catch (IOException e) {
        log.error(String.format("IOException caught while attempt to re-index WARC file %s", warcFile));
        throw e;
      }
    }

    // TODO: What follows is loading of artifact repository-specific metadata. It should be generalized to others.

    // Get a collection of paths to WARCs containing repository metadata
    Collection<String> repoMetadataWarcFiles = warcPaths
        .stream()
        .filter(file -> file.endsWith("lockss-repo" + WARC_FILE_EXTENSION))
        .collect(Collectors.toList());

    // Load repository artifact metadata by "replaying" them
    for (String metadataFile : repoMetadataWarcFiles) {
      try {
        BufferedInputStream bufferedStream = new BufferedInputStream(getInputStream(metadataFile));
        for (ArchiveRecord record : WARCReaderFactory.get("WarcArtifactDataStore", bufferedStream, true)) {
          // Parse the JSON into a RepositoryArtifactMetadata object
          RepositoryArtifactMetadata repoState = new RepositoryArtifactMetadata(
              IOUtils.toString(record, "UTF-8")
          );

          String artifactId = repoState.getArtifactId();

          log.info(String.format(
              "Replaying repository metadata for artifact %s, from WARC %s record %s in %s",
              artifactId,
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
              record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
              metadataFile
          ));

          if (index.artifactExists(artifactId)) {
            if (repoState.isDeleted()) {
              log.info(String.format("Removing artifact %s from index", artifactId));
              index.deleteArtifact(artifactId);
              continue;
            }

            if (repoState.isCommitted()) {
              log.info(String.format("Marking artifact %s as committed in index", artifactId));
              index.commitArtifact(artifactId);
            }
          } else {
            if (!repoState.isDeleted()) {
              log.warn(String.format("Artifact %s not found in index; skipped replay", artifactId));
            }
          }
        }
      } catch (IOException e) {
        log.error(String.format(
            "IOException caught while attempt to re-index metadata WARC file %s",
            metadataFile
        ));

        throw e;
      }
    }
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
        String uri = System.getProperty(JmsConsumer.SYSPROP_JMS_URI);
        log.trace("uri: {}", uri);

        if (uri == null || uri.trim().isEmpty()) {
          return;
        }

        log.info("Establishing JMS connection with {}", uri);
        while (jmsConsumer == null) {
          try {
            log.trace("Attempting to establish JMS connection with {}", uri);
            jmsConsumer = JmsConsumer.createTopicConsumer(CLIENT_ID, JMS_TOPIC, new DataStoreCrawlListener("AuEvent Listener"));
            log.info("Successfully established JMS connection with {}", uri);
          } catch (JMSException | NullPointerException exc) {
            log.trace("Could not establish JMS connection with {}; sleeping and retrying", uri);
            TimerUtil.guaranteedSleep(retryDelay);
          }
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

  private class DataStoreCrawlListener extends JmsConsumer.SubscriptionListener {

    DataStoreCrawlListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {

      try {
        Map<String, Object> msgMap = (Map<String, Object>) JmsConsumer.convertMessage(message);
        String auId = (String) msgMap.get(KEY_AUID);
        String event = (String) msgMap.get(KEY_TYPE);
        String repospec = (String) msgMap.get(KEY_REPO_SPEC);
        log.debug("Received notification: [AuEvent: {} AU: {} change: {}]", event, auId, repospec);
        if (auId != null && repospec != null && CONTENT_CHANGED.equals(event)) {
          Matcher m1 = REPO_SPEC_PATTERN.matcher(repospec);
          if (m1.matches()) {
            String collection = m1.group(2);
            if (collection != null) {
              execsvc.submit(new SealActiveWarcTask(collection, auId));
            }
          }
        }
      } catch (JMSException e) {
        log.error("Unable to understand crawl listener jms message.");
      }
    }
  }

}
