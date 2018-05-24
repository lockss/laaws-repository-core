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

import java.io.*;
import java.net.*;
import java.security.*;
import java.time.*;
import java.time.format.*;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.*;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.*;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.Element;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.io.CountingOutputStream;

/**
 * An abstract class that implements methods common to WARC implementations of ArtifactDataStore.
 */
public abstract class WarcArtifactDataStore implements ArtifactDataStore<ArtifactIdentifier, ArtifactData, RepositoryArtifactMetadata>, WARCConstants {

  private final static Log log = LogFactory.getLog(WarcArtifactDataStore.class);

  // DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS") does not parse in Java 8: https://bugs.openjdk.java.net/browse/JDK-8031085
  protected static final DateTimeFormatter FMT_TIMESTAMP =
      new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss")
                                    .appendValue(ChronoField.MILLI_OF_SECOND, 3)
                                    .toFormatter()
                                    .withZone(ZoneId.of("UTC"));
  
    protected static final String WARC_FILE_EXTENSION = ".warc";
    protected static final String COLLECTIONS_DIR = "collections";
    protected static final String SEALED_WARC_DIR = "sealed";
    protected static final String AU_DIR_PREFIX = "au-";
    protected static final String AU_ARTIFACTS_WARC_NAME = "artifacts" + WARC_FILE_EXTENSION;
    protected static final byte[] SEALED_MARK = "This WARC file is marked as sealed and should contain no subsequent records.".getBytes();

    protected static final String SCHEME = "urn:uuid";
    protected static final String SCHEME_COLON = SCHEME + ":";
    protected static final String CRLF = "\r\n";
    protected static byte[] CRLF_BYTES;
    protected static String SEPARATOR = "/";

    protected static final String DEFAULT_DIGEST_ALGORITHM = "SHA-256";

    protected static final long DEFAULT_THRESHOLD_WARC_SIZE = 100L * FileUtils.ONE_MB;

    protected ArtifactIndex artifactIndex;
    
    protected String basePath;

    protected long thresholdWarcSize;

    protected Pattern fileAndOffsetStorageUrlPat;
    
    static {
        try {
            CRLF_BYTES = CRLF.getBytes(DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    protected WarcArtifactDataStore() {
      // nothing
      this.thresholdWarcSize = DEFAULT_THRESHOLD_WARC_SIZE;
    }
    
    public WarcArtifactDataStore(String basePath) {
      this();
      this.basePath = basePath;
    }
    
  public String getBasePath() {
    return basePath;
  }
    
  public void setThresholdWarcSize(long threshold) {
    if (threshold < 1L) {
      throw new IllegalArgumentException("Threshold size must be strictly positive");
    }
    this.thresholdWarcSize = threshold;
  }

    @Override
    public void setArtifactIndex(ArtifactIndex artifactIndex) {
      if (this.artifactIndex != null) {
        throw new IllegalStateException("Artifact index already set");
      }
      this.artifactIndex = artifactIndex;
    }
    
  @Override
  public Artifact addArtifactData(ArtifactData artifactData) throws IOException {
    Objects.requireNonNull(artifactData, "Artifact data is null");
      
    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    // Set new artifactId - any existing artifactId is meaningless in this context and should be discarded
    artifactId.setId(UUID.randomUUID().toString());

    log.info(String.format(
        "Adding artifact (%s, %s, %s, %s, %s)",
        artifactId.getId(),
        artifactId.getCollection(),
        artifactId.getAuid(),
        artifactId.getUri(),
        artifactId.getVersion()
    ));

    String auArtifactsWarcPath = getAuArtifactsWarcPath(artifactId);
    log.info(String.format("Appending artifact data to %s", auArtifactsWarcPath));
    createFileIfNeeded(auArtifactsWarcPath);

    // Set the offset for the record to be appended to the length of the WARC file (i.e., the end)
    long offset = getFileLength(auArtifactsWarcPath);
    long bytesWritten;

    try (
      // Get an appending OutputStream to the WARC file
      OutputStream out = getAppendableOutputStream(auArtifactsWarcPath);
    ) {
      // Write artifact to WARC file
      bytesWritten = writeArtifactData(artifactData, out);
      out.flush();
    } catch (HttpException e) {
      throw new IOException("Caught HttpException while attempting to write artifact to WARC file", e);
    }

    log.info(String.format(
            "Wrote %d bytes starting at byte offset %d to %s; size is now %d",
            bytesWritten,
            offset,
            auArtifactsWarcPath,
            offset + bytesWritten
    ));

    // Determine if a WARC seal was triggered by WARC size threshold; set artifact data's storage URL as appropriate
    long warcFileLength = offset + bytesWritten;
    if (warcFileLength >= thresholdWarcSize) {
      log.info(String.format(
              "Seal WARC triggered by size threshold [Threshold: %d bytes, Length: %d bytes] for [Collection: %s, AUID: %s]",
              warcFileLength,
              thresholdWarcSize,
              artifactId.getCollection(),
              artifactId.getAuid()
      ));

      // Seal the artifacts WARC
      String newPath = sealWarc(artifactId.getCollection(), artifactId.getAuid());

      // Set the storage URL of the artifact we're adding to the sealed WARC path
      artifactData.setStorageUrl(makeStorageUrl(newPath, offset));
    } else {
      // Set the storage URL of the artifact we're adding to default artifacts WARC path
      artifactData.setStorageUrl(makeStorageUrl(auArtifactsWarcPath, offset));
    }
        
    // Attach the artifact's repository metadata
    artifactData.setRepositoryMetadata(new RepositoryArtifactMetadata(
                artifactId,
                false,
                false
    ));

    // Write artifact data metadata - TODO: Generalize this to write all of an artifact's metadata
    createFileIfNeeded(getAuMetadataWarcPath(artifactId, artifactData.getRepositoryMetadata()));
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
    log.info(String.format("Retrieving artifact from store (artifactId: %s)", artifact.getId()));

    String storageUrl = artifact.getStorageUrl();

    ArtifactData artifactData;
    try (
      InputStream warcStream = getWarcRecordInputStream(storageUrl);
    ) {
      WARCRecord warcRecord = new WARCRecord(warcStream, getClass().getSimpleName() + "#getArtifactData", 0);
      // Convert the WARCRecord object to an ArtifactData
      artifactData = ArtifactDataFactory.fromArchiveRecord(warcRecord);
    }

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
  
  @Override
  public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier ident,
                                                           RepositoryArtifactMetadata artifactMetadata)
      throws IOException {
    Objects.requireNonNull(ident, "Artifact identifier is null");
    Objects.requireNonNull(artifactMetadata, "Repository artifact metadata is null");
    WARCRecordInfo metadataRecord = createWarcMetadataRecord(ident.getId(), artifactMetadata);

    // Append WARC metadata record to AU's metadata file
    String auMetadataWarcPath = getAuMetadataWarcPath(ident, artifactMetadata);
    try (
      OutputStream out = getAppendableOutputStream(auMetadataWarcPath);
    ) {
      writeWarcRecord(metadataRecord, out);
    }

    return artifactMetadata;
  }
    
  @Override
  public RepositoryArtifactMetadata commitArtifactData(Artifact artifact) throws IOException {
    Objects.requireNonNull(artifact, "Artifact is null");
    ArtifactData artifactData = getArtifactData(artifact);
    RepositoryArtifactMetadata repoMetadata = artifactData.getRepositoryMetadata();

    // Set the commit flag and write the metadata to disk
    if (!repoMetadata.isDeleted()) {
      repoMetadata.setCommitted(true);
      updateArtifactMetadata(artifact.getIdentifier(), repoMetadata);
    }

    return repoMetadata;
  }

  @Override
  public RepositoryArtifactMetadata deleteArtifactData(Artifact artifact) throws IOException {
    Objects.requireNonNull(artifact, "Artifact is null");
    ArtifactData artifactData = getArtifactData(artifact);
    RepositoryArtifactMetadata repoMetadata = artifactData.getRepositoryMetadata();

    // Set the commit flag and write the metadata to disk
    if (!repoMetadata.isDeleted()) {
      repoMetadata.setCommitted(false);
      repoMetadata.setDeleted(true);
      updateArtifactMetadata(artifact.getIdentifier(), repoMetadata);
    }

    return repoMetadata;
  }

    public String sealWarc(String collection, String auid) throws IOException {
        // Current and new (sealed) path of artifacts WARC file
        String currentPath = getAuArtifactsWarcPath(collection, auid);
        String newPath = getSealedWarcPath() + SEPARATOR + getSealedWarcName(collection, auid);

        log.info(String.format("Sealing WARC %s", currentPath));

        // Write terminating warcinfo record
        writeTerminateWarcFile(currentPath);

        // Move the default artifacts.warc file to the sealed WARCs directory
        log.info(String.format("Moving sealed WARC %s to %s", currentPath, newPath));
        renameFile(currentPath, newPath);

        // Iterate over all the artifacts in the sealed WARC and update their storage URLs in the index
        for (Artifact artifact : artifactIndex.getAllArtifactsAllVersions(collection, auid)) {
            String newStorageUrl = makeNewStorageUrl(newPath, artifact);

            log.info(String.format(
                    "Updating storage URL for artifact %s: %s -> %s",
                    artifact.getId(),
                    artifact.getStorageUrl(),
                    newStorageUrl
            ));

            if (newStorageUrl != null) {
                artifactIndex.updateStorageUrl(artifact.getId(), newStorageUrl);
            }
        }


        return newPath;
    }

    private void writeTerminateWarcFile(String warcfilePath) throws IOException {
        OutputStream warcOutput = getAppendableOutputStream(warcfilePath);
        writeWarcRecord(createWARCInfoRecord(null, MediaType.TEXT_PLAIN, SEALED_MARK), warcOutput);
        warcOutput.flush();
        warcOutput.close();
    }

    public String getCollectionPath(String collection) {
      return SEPARATOR + COLLECTIONS_DIR + SEPARATOR + collection;
    }

    public String getCollectionPath(ArtifactIdentifier artifactIdent) {
      return getCollectionPath(artifactIdent.getCollection());
    }

    public String getAuPath(String collection, String auid) {
      return getCollectionPath(collection) + SEPARATOR + AU_DIR_PREFIX + DigestUtils.md5Hex(auid);
    }
    
    public String getAuPath(ArtifactIdentifier artifactIdent) {
      return getAuPath(artifactIdent.getCollection(), artifactIdent.getAuid());
    }
    
    public String getSealedWarcPath() {
      return SEPARATOR + SEALED_WARC_DIR;
    }
    
    public String getSealedWarcName(String collection, String auid) {
      String auidHash = DigestUtils.md5Hex(auid);
      String timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(FMT_TIMESTAMP);
      return collection + "_" + AU_DIR_PREFIX + auidHash + "_" + timestamp + AU_ARTIFACTS_WARC_NAME;
    }
    
    public String getAuArtifactsWarcPath(String collection, String auid) throws IOException {
      String dir = getAuPath(collection, auid);
      mkdirsIfNeeded(dir);
      return dir + SEPARATOR + AU_ARTIFACTS_WARC_NAME;
    }

    public String getAuArtifactsWarcPath(ArtifactIdentifier artifactIdent) throws IOException {
      return getAuArtifactsWarcPath(artifactIdent.getCollection(), artifactIdent.getAuid());
    }

    public String getAuMetadataWarcPath(ArtifactIdentifier ident,
                                        RepositoryArtifactMetadata artifactMetadata)
        throws IOException {
      String dir = getAuPath(ident);
      mkdirsIfNeeded(dir);
      return dir + SEPARATOR + artifactMetadata.getMetadataId() + WARC_FILE_EXTENSION;
    }
    
    public String makeStorageUrl(String filePath, long offset) {
      return makeStorageUrl(filePath, Long.toString(offset));
    }

    public String makeStorageUrl(String filePath) {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        return makeStorageUrl(filePath, params);
    }

    public abstract String makeStorageUrl(String filePath, String offset);

    public abstract String makeStorageUrl(String filePath, MultiValueMap<String,String> params);

    public abstract String makeNewStorageUrl(String newPath, Artifact artifact);
    
    public abstract void mkdirsIfNeeded(String dirPath) throws IOException;
    
    public abstract long getFileLength(String filePath) throws IOException;
    
    public abstract OutputStream getAppendableOutputStream(String filePath) throws IOException;
    
    public abstract InputStream getInputStream(String filePath) throws IOException;
    
    public abstract InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException;
    
    public abstract InputStream getWarcRecordInputStream(String storageUrl) throws IOException;
    
    public abstract void createFileIfNeeded(String filePath) throws IOException;
    
    public abstract void renameFile(String srcPath, String dstPath) throws IOException;
    
    /**
     * Returns the WARC-Record-Id of the WARC record backing a given ArtifactData.
     *
     * @param file URL to a WARC file.
     * @param offset Absolute byte offset of WARC record in WARC file.
     * @return The value of the mandatory WARC-Record-Id header.
     * @throws IOException
     */
    public static String getWarcRecordId(URL file, long offset) throws IOException {
        WARCRecord record = getWarcRecord(file, offset);
        ArchiveRecordHeader recordHeader = record.getHeader();
        return (String) recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_ID);
    }

    /**
     * Returns a WARCRecord object representing the WARC record at a given URL and offset.
     *
     * Support for different protocols is handled by implementing URLConnection, URLStreamHandler, and
     * URLStreamHandlerFactory, then registering the custom URLStreamHandlerFactory with URL#setURLStreamHandlerFactory.
     *
     * @param file URL to a WARC file.
     * @param offset Absolute byte offset of WARC record in WARC file.
     * @return A WARCRecord object representing the WARC record in the WARC file, at the given offset.
     * @throws IOException
     */
    public static WARCRecord getWarcRecord(URL file, long offset) throws IOException {
        InputStream warcStream = file.openStream();
        warcStream.skip(offset);
        return new WARCRecord(file.openStream(), "WarcArtifactDataStore", 0);
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
    public static long writeArtifactData(ArtifactData artifactData, OutputStream outputStream) throws IOException, HttpException {
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

        // Add LOCKSS-specific WARC headers to record (Note: X-Lockss-ArtifactId and X-Lockss-Uri are redundant because
        // the same information is recorded as WARC-Record-ID and WARC-Target-URI, respectively).
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_ID_KEY, artifactId.getId());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_COLLECTION_KEY, artifactId.getCollection());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_AUID_KEY, artifactId.getAuid());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_URI_KEY, artifactId.getUri());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_VERSION_KEY, String.valueOf(artifactId.getVersion()));

        // We're required to pre-compute the WARC payload (which is an artifact encoded as an HTTP response stream) but
        // it is not possible to determine the final size without reading the InputStream entirely, so we use a
        // DeferredFileOutputStream, copy the InputStream into it, and determine the number of bytes written.
        DeferredFileOutputStream dfos = new DeferredFileOutputStream(16384, "artifactData", null, new File("/tmp"));

        // Wrap the artifact content stream in a CountingInputStream
        CountingInputStream cis = new CountingInputStream(artifactData.getInputStream());

        // Will hold a DigestInputStream to comptue the artifact data shortly
        DigestInputStream dis = null;

        try {
            // Wrap the stream in a DigestInputStream
            dis = new DigestInputStream(cis, MessageDigest.getInstance(DEFAULT_DIGEST_ALGORITHM));
        } catch (NoSuchAlgorithmException e) {
            String errMsg = String.format(
                    "Unknown digest algorithm: %s; could not instantiate a MessageDigest", DEFAULT_DIGEST_ALGORITHM
            );

            log.error(errMsg);
            // Clean up resources.
            cis.close();
            dfos.close();
            throw new RuntimeException(errMsg);
        }

        // Create a HTTP response stream from the ArtifactData
        InputStream httpResponse = ArtifactDataUtil.getHttpResponseStreamFromHttpResponse(
                ArtifactDataUtil.getHttpResponseFromArtifact(
                        artifactData.getIdentifier(),
                        artifactData.getHttpStatus(),
                        artifactData.getMetadata(),
                        dis
                )
        );

        IOUtils.copy(httpResponse, dfos);

        // Get the temporary file created, if it exists, so that we can delete
        // it after it's no longer needed.
        File dfosFile = dfos.getFile();

        dfos.close();

        // Set the length of the artifact data
        long contentLength = cis.getByteCount();
        if (log.isDebugEnabled()) log.debug("contentLength = " + contentLength);
        artifactData.setContentLength(contentLength);
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_CONTENT_LENGTH_KEY,
            String.valueOf(contentLength));

        cis.close();

        // Set content digest of artifact data
        String contentDigest = String.format("%s:%s",
            dis.getMessageDigest().getAlgorithm(),
            new String(Hex.encodeHex(dis.getMessageDigest().digest())));
        if (log.isDebugEnabled()) log.debug("contentDigest = " + contentDigest);
        artifactData.setContentDigest(contentDigest);
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_DIGEST_KEY,
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
     * @param out An OutputStream.
     * @throws IOException
     */
    public static void writeWarcRecord(WARCRecordInfo record, OutputStream out) throws IOException {
        // Write the header
        out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

        // Write the header-body separator
        out.write(CRLF_BYTES);

        // Write the body
        if (record.getContentStream() != null) {
            // TODO: Put logic here to check and enforce WARC file lengths
            int bytesWritten = IOUtils.copy(record.getContentStream(), out);
            if (bytesWritten != record.getContentLength())
                log.warn(String.format("Expected %d bytes, but wrote %d", record.getContentLength(), bytesWritten));
        }

        // Write the two blank lines at end of all records
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
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
        sb.append(HEADER_KEY_ID).append(COLON_SPACE).append('<').append(SCHEME_COLON).append(record.getRecordId().toString()).append('>').append(CRLF);
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

            for (final Iterator<Element> i = record.getExtraHeaders().iterator(); i.hasNext();) {
                sb.append(i.next()).append(CRLF);
            }
        }

        return sb.toString();
    }

    /**
     * Creates a warcinfo type WARC record with metadata common to all following WARC records.
     *
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
        record.setContentLength(content == null ? 0 : (long)content.length);
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

    /**
     * Generates a new UUID for use as the WARC-Record-Id for a WARC record.
     *
     * @return A new UUID.
     */
    private static URI generateRecordId() {
        try {
            return new URI(SCHEME_COLON + UUID.randomUUID().toString());
        } catch (URISyntaxException e) {
            // This should never happen
            throw new RuntimeException(e);
        }
    }
    
    public static URI urlToUri(String url) throws IllegalStateException {
      try {
        return new URI(url);
      }
      catch (URISyntaxException exc) {
        throw new IllegalStateException("Internal error converting to URI: " + url, exc);
      }
    }


    protected String makeNewFileAndOffsetStorageUrl(String newPath, Artifact artifact) {
        Matcher mat = fileAndOffsetStorageUrlPat.matcher(artifact.getStorageUrl());

        try {
            if (mat.matches() && mat.group(3).equals(getAuArtifactsWarcPath(artifact.getIdentifier()))) {
                return makeStorageUrl(newPath, mat.group(4));
            }
        } catch (IOException e) {
            // Shouldn't happen because all these artifacts are in existing directories
            log.error("Internal error", e);
            throw new UncheckedIOException(e);
        }

        return null;
    }

    /**
     * Parses a storage URL containing a byte offset
     * @param storageUrl
     * @return
     * @throws IOException
     */
  protected InputStream getFileAndOffsetWarcRecordInputStream(String storageUrl) throws IOException {
    Matcher mat = fileAndOffsetStorageUrlPat.matcher(storageUrl);
    if (!mat.matches()) {
      // Shouldn't happen because all these artifacts are in existing directories
      String msg = "Internal error: " + storageUrl;
      log.error(msg);
      throw new IOException(msg);
    }
    return getInputStreamAndSeek(mat.group(3), Long.parseLong(mat.group(4)));
  }


    public void rebuildIndex() throws IOException {
        if (artifactIndex != null)
            this.rebuildIndex(this.artifactIndex);

        throw new IllegalStateException("No artifact index set");
    }

    /**
     * Rebuilds the index by traversing a repository base path for artifacts and metadata WARC files.
     *
     * @param index
     *          An ArtifactIndex to rebuild and populate from WARCs.
     * @throws IOException
     */
    public void rebuildIndex(ArtifactIndex index) throws IOException {
        rebuildIndex(index, getBasePath());
    }

    public abstract Collection<String> scanDirectories(String basePath) throws IOException;

    /**
     * Rebuilds the index by traversing a repository base path for artifacts and metadata WARC files.
     *
     * @param basePath The base path of the local repository.
     * @throws IOException
     */
    public void rebuildIndex(ArtifactIndex index, String basePath) throws IOException {
        Collection<String> warcPaths = scanDirectories(basePath);

        // Build a collection of paths to WARCs containing artifact data
        Collection<String> artifactWarcFiles = warcPaths
                .stream()
                .filter(file -> file.endsWith("artifacts" + WARC_FILE_EXTENSION))
                .collect(Collectors.toList());

        // Re-index artifacts first
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
                            IOUtils.toString(record)
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
}
