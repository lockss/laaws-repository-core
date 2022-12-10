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

package org.lockss.laaws.rs.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.IdentityInputStream;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.message.BasicHeader;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.io.storage.warc.ArtifactState;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.ArtifactProperties;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.multipart.MultipartMessage;
import org.lockss.util.rest.multipart.MultipartResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ArtifactData factory: Instantiates ArtifactData objects from a variety of sources.
 */
public class ArtifactDataFactory {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Instantiates an {@code ArtifactData} from an {@code InputStream} containing the byte stream of an HTTP response.
   *
   * @param responseStream An {@code InputStream} containing an HTTP response byte stream which in turn encodes an artifact.
   * @return An {@code ArtifactData} representing the artifact encoded in an HTTP response input stream.
   * @throws IOException
   */
  public static ArtifactData fromHttpResponseStream(InputStream responseStream) throws IOException {
    if (responseStream == null) {
      throw new IllegalArgumentException("InputStream is null");
    }

    return fromHttpResponseStream(null, responseStream);
  }

  /**
   * Instantiates an {@code ArtifactData} from an {@code InputStream} containing the byte stream of an HTTP response.
   * <p>
   * Allows additional HTTP headers to be injected by passing a {@code HttpHeaders}.
   *
   * @param additionalMetadata A {@code HttpHeader} with additional headers.
   * @param responseStream     An {@code InputStream} containing an HTTP response byte stream which in turn encodes an artifact.
   * @return An {@code ArtifactData} representing the artifact encoded in an HTTP response input stream.
   * @throws IOException
   */
  public static ArtifactData fromHttpResponseStream(HttpHeaders additionalMetadata, InputStream responseStream)
      throws IOException {
    // Attach remaining data in stream as the response entity. We cannot use InputStreamEntity directly because
    // it is now wrapped within a SessionInputBufferImpl so we instantiate a BasicHttpEntity and populate it an
    // IdentityInputStream. We could also have used StrictContentLengthStrategy and ContentLengthInputStream.
    try {
      HttpResponse response = getHttpResponseFromStream(responseStream);

      // Merge additional artifact metadata into HTTP response header
      if (additionalMetadata != null) {
        additionalMetadata.forEach((headerName, headerValues) ->
            headerValues.forEach((headerValue) -> response.setHeader(headerName, headerValue)
            ));
      }

      return fromHttpResponse(response);
    } catch (HttpException e) {
      log.error(
          String.format("An error occurred while attempting to parse a stream as a HTTP response: %s", e)
      );

      throw new IOException(e);
    }
  }

  /**
   * Adapts an {@code InputStream} with an HTTP response into an Apache {@code HttpResponse} object.
   *
   * @param inputStream An {@code InputStream} containing an HTTP response to parse.
   * @return A {@code HttpResponse} representing the HTTP response in the {@code InputStream}.
   * @throws HttpException
   * @throws IOException
   */
  public static HttpResponse getHttpResponseFromStream(InputStream inputStream) throws HttpException, IOException {
    // Create a SessionInputBuffer from the InputStream containing a HTTP response
    SessionInputBufferImpl buffer = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 4096);
    buffer.bind(inputStream);

    // Parse the InputStream to a HttpResponse object
    HttpResponse response = (new DefaultHttpResponseParser(buffer)).parse();
//        long len = (new LaxContentLengthStrategy()).determineLength(response);

    // Create and attach an HTTP entity to the HttpResponse
    BasicHttpEntity responseEntity = new BasicHttpEntity();
//        responseEntity.setContentLength(len);
    responseEntity.setContent(new IdentityInputStream(buffer));
    response.setEntity(responseEntity);

    return response;
  }

  /**
   * Instantiates an {@code ArtifactData} from a Apache {@code HttpResponse} object.
   *
   * @param response A {@code HttpResponse} object containing an artifact.
   * @return An {@code ArtifactData} representing the artifact encoded in the {@code HttpResponse} object.
   * @throws IOException
   */
  public static ArtifactData fromHttpResponse(HttpResponse response) throws IOException {
    if (response == null) {
      throw new IllegalArgumentException("HttpResponse is null");
    }

    HttpHeaders headers = transformHeaderArrayToHttpHeaders(response.getAllHeaders());

    ArtifactData artifactData = new ArtifactData(
        null,
        headers,
        response.getEntity().getContent(),
        response.getStatusLine());

//        artifactData.setContentLength(response.getEntity().getContentLength());

    return artifactData;
  }

  /**
   * Instantiates an {@code ArtifactIdentifier} from HTTP headers in a {@code HttpHeaders} object.
   *
   * @param props An {@code Map} object representing HTTP headers containing an artifact identity.
   * @return An {@code ArtifactIdentifier}.
   */
  public static ArtifactIdentifier buildArtifactIdentifier(Map<String, String> props) {
    int version = 0;

    String versionVal = props.get(Artifact.ARTIFACT_VERSION_KEY);
    if (!StringUtil.isNullOrEmpty(versionVal)) {
      version = Integer.parseInt(versionVal);
    }

    return new ArtifactIdentifier(
        props.get(Artifact.ARTIFACT_UUID_KEY),
        props.get(Artifact.ARTIFACT_NAMESPACE_KEY),
        props.get(Artifact.ARTIFACT_AUID_KEY),
        props.get(Artifact.ARTIFACT_URI_KEY),
        version);
  }

  public static ArtifactIdentifier buildArtifactIdentifier(ArtifactProperties props) {
    return new ArtifactIdentifier(
        props.getUuid(),
        props.getNamespace(),
        props.getAuid(),
        props.getUri(),
        props.getVersion());
  }

  /**
   * Instantiates an {@code ArtifactIdentifier} from headers in a ARC / WARC {@code ArchiveRecordHeader} object.
   *
   * @param headers An {@code ArchiveRecordHeader} ARC / WARC header containing an artifact identity.
   * @return An {@code ArtifactIdentifier}.
   */
  public static ArtifactIdentifier buildArtifactIdentifier(ArchiveRecordHeader headers) {
    int version = 0;

    String versionHeader = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_VERSION_KEY);
    if (!StringUtils.isEmpty(versionHeader)) {
      version = Integer.parseInt(versionHeader);
    }

    String namespace = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_NAMESPACE_KEY);
    if (StringUtils.isEmpty(namespace)) {
      namespace = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_COLLECTION_KEY);
    }

    return new ArtifactIdentifier(
        parseWarcRecordIdForUUID((String)headers.getHeaderValue(WARCConstants.HEADER_KEY_ID)),
        namespace,
        (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_AUID_KEY),
        // Q: Use (String)headers.getHeaderValue(WARCConstants.HEADER_KEY_URI)?
        (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_URI_KEY),
        version);
  }

  private final static Pattern uuidPattern = Pattern.compile("<urn:uuid:(.+)>");

   public static String parseWarcRecordIdForUUID(String recordId) {
    Matcher result = uuidPattern.matcher(recordId);

    if (result.matches()) {
      // Validate UUID class
      return UUID.fromString(result.group(1)).toString();
    }

    throw new IllegalArgumentException("Unexpected WARC-Record-ID: " + recordId);
  }

  /**
   * Returns the value from an {@code HttpHeaders} object for a given key.
   * <p>
   * The value must for this key must be unique.
   *
   * @param headers A {@code HttpHeaders} to return the key's value from.
   * @param key     A {@code String} containing the key of the value to return.
   * @return A {@code String} value, or {@code null} if this key is not found or has multiple values.
   */
  private static String getHeaderValue(HttpHeaders headers, String key) {
    List<String> values = headers.get(key);

    if ((values != null) && !values.isEmpty()) {
      // Allow duplicates of the header as long as the value is the same
      if (values.stream().allMatch(values.get(0)::equals)) {
        return values.get(0);
      }
    }

    // TODO: Should this throw instead?
    return null;
  }

  /**
   * Reorganizes an array of Apache Header objects into a single Spring HttpHeaders object.
   *
   * @param headerArray An array of {@code Header} objects to reorganize.
   * @return A Spring {@code HttpHeaders} object representing the array of Apache {@code Header} objects.
   */
  // TODO: Move this to lockss-util?
  public static HttpHeaders transformHeaderArrayToHttpHeaders(Header[] headerArray) {
    HttpHeaders headers = new HttpHeaders();
    Arrays.stream(headerArray).forEach(header -> headers.add(header.getName(), header.getValue()));

    return headers;
  }

  public static Header[] transformHttpHeadersToHeaderArray(HttpHeaders headers) {
    Header[] result = headers.entrySet()
        .stream()
        .flatMap(entry -> entry.getValue()
            .stream()
            .map(v -> new BasicHeader(entry.getKey(), v)))
        .toArray(Header[]::new);

    return result;
  }

  /**
   * Instantiates an {@code ArtifactData} from an arbitrary byte stream in an {@code InputStream}.
   * <p>
   * Uses a default HTTP response status of HTTP/1.1 200 OK.
   *
   * @param resourceStream An {@code InputStream} containing the byte stream to instantiate an {@code ArtifactData} from.
   * @return An {@code ArtifactData} wrapping the byte stream.
   */
  public static ArtifactData fromResource(InputStream resourceStream) {
    return fromResourceStream(null, resourceStream);
  }

  /**
   * Constructs an {@link ArtifactData} from an arbitrary byte stream in an {@link InputStream}.
   *
   * @param headers       A Spring {@link HttpHeaders} object containing optional artifact headers.
   * @param resourceStream An {@link InputStream} containing an arbitrary byte stream.
   * @return An {@link ArtifactData} wrapping the byte stream.
   */
  public static ArtifactData fromResourceStream(HttpHeaders headers, InputStream resourceStream) {
    return fromResourceStream(headers, resourceStream, null);
  }

  /**
   * Instantiates an {@code ArtifactData} from an arbitrary byte stream in an {@code InputStream}.
   * <p>
   * Takes a {@code StatusLine} containing the HTTP response status associated with this byte stream.
   *
   * @param metadata       A Spring {@code HttpHeaders} object containing optional artifact headers.
   * @param resourceStream An {@code InputStream} containing an arbitrary byte stream.
   * @param responseStatus
   * @return An {@code ArtifactData} wrapping the byte stream.
   */
  public static ArtifactData fromResourceStream(HttpHeaders metadata, InputStream resourceStream, StatusLine responseStatus) {
    return new ArtifactData(metadata, resourceStream, responseStatus);
  }

  /**
   * Instantiates an {@code ArtifactData} from an ARC / WARC {@code ArchiveRecord} object containing an artifact.
   *
   * @param record An {@code ArchiveRecord} object containing an artifact.
   * @return An {@code ArtifactData} representing the artifact contained in the {@code ArchiveRecord}.
   * @throws IOException
   */
  public static ArtifactData fromArchiveRecord(ArchiveRecord record) throws IOException {
    // Get WARC record header
    ArchiveRecordHeader recordHeader = record.getHeader();

    ArtifactData ad;

    // Read ArtifactIdentifier from the WARC record headers
    ArtifactIdentifier artifactId = buildArtifactIdentifier(recordHeader);

    // Read WARC record type from record headers
    WARCConstants.WARCRecordType recordType =
        WARCConstants.WARCRecordType.valueOf((String) recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_TYPE));

    // Artifacts can only be read out of WARC response and resource type records
    switch (recordType) {
      case response:
        // Sanity check
        String mimeType = recordHeader.getMimetype();
        if (!mimeType.startsWith("application/http")) {
          log.warn("Unexpected content MIME type WARC response record: {}", mimeType);
          throw new IllegalStateException("Invalid MIME type: " + mimeType);
        }

        // Parse the ArchiveRecord into an ArtifactData
        ad = ArtifactDataFactory.fromHttpResponseStream(record);
        ad.setIdentifier(artifactId);
        break;

      case resource:
        // Parse the ArchiveRecord into an ArtifactData
        ad = ArtifactDataFactory.fromResource(record);
        ad.setIdentifier(artifactId);

        // Set the ArtifactData content-type to that of the WARC record block if present
        String typeVal = recordHeader.getMimetype();
        if (!StringUtil.isNullOrEmpty(typeVal)) {
          ad.getHttpHeaders().set(HttpHeaders.CONTENT_TYPE, typeVal);
        }
        break;

      default:
        log.warn("Unexpected WARC record type [WARC-Record-ID: {}, WARC-Type: {}]",
            recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_ID), recordType);

        // Could not return an artifact elsewhere
        return null;
    }

    String artifactContentLength = (String) recordHeader.getHeaderValue(ArtifactConstants.ARTIFACT_LENGTH_KEY);
    log.trace("artifactContentLength = {}", artifactContentLength);
    if (artifactContentLength != null && !artifactContentLength.trim().isEmpty()) {
      ad.setContentLength(Long.parseLong(artifactContentLength));
    }

    String artifactDigest = (String) recordHeader.getHeaderValue(ArtifactConstants.ARTIFACT_DIGEST_KEY);
    log.trace("artifactDigest = {}", artifactDigest);
    if (artifactDigest != null && !artifactDigest.trim().isEmpty()) {
      ad.setContentDigest(artifactDigest);
    }

    String artifactStoredDate = (String) recordHeader.getHeaderValue(ArtifactConstants.ARTIFACT_STORED_DATE);
    log.trace("artifactStoredDate = {}", artifactStoredDate);
    if (artifactStoredDate != null && !artifactStoredDate.trim().isEmpty()) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactStoredDate);
      ad.setStoredDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    String artifactCollectionDate = (String) recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_DATE);
    log.trace("artifactCollectionDate = {}", artifactCollectionDate);
    if (artifactCollectionDate != null && !artifactCollectionDate.trim().isEmpty()) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactCollectionDate);
      ad.setCollectionDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    log.trace("ad = {}", ad);

    return ad;
  }

  public static ArtifactData fromTransportResponseEntity(ResponseEntity<MultipartMessage> response) throws IOException {
    try {
      // For JSON object parsing
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      // Assemble ArtifactData object from multipart response parts
      MultipartResponse multipartMessage = new MultipartResponse(response);
      LinkedHashMap<String, MultipartResponse.Part> parts = multipartMessage.getParts();
      ArtifactData result = new ArtifactData();

      //// Set artifact repository properties
      {
        MultipartResponse.Part part = parts.get(RestLockssRepository.MULTIPART_ARTIFACT_PROPS);

        ArtifactProperties props = mapper.readValue(part.getInputStream(), ArtifactProperties.class);

        // Set ArtifactIdentifier
        ArtifactIdentifier id = buildArtifactIdentifier(props);

        result.setIdentifier(id);

        String stateVal = props.getState();
        if (!StringUtil.isNullOrEmpty(stateVal)) {
          result.setArtifactState(ArtifactState.valueOf(stateVal));
        }

        // Set misc. artifact properties
        result.setContentLength(props.getContentLength());
        result.setContentDigest(props.getContentDigest());
      }

      //// Set artifact HTTP response status and headers
      {
        MultipartResponse.Part part = parts.get(RestLockssRepository.MULTIPART_ARTIFACT_HTTP_RESPONSE_HEADER);

        // Parse header part body into HttpHeaders object
        if (part != null) {
          try {
            HttpResponse httpResponse =
                ArtifactDataFactory.getHttpResponseFromStream(part.getInputStream());

            // Set HTTP status
            result.setHttpStatus(httpResponse.getStatusLine());

            // Set HTTP headers
            result.setHttpHeaders(ArtifactDataFactory.transformHeaderArrayToHttpHeaders(httpResponse.getAllHeaders()));
          } catch (HttpException e) {
            throw new IOException("Error parsing HTTP response header part", e);
          }
        }
      }

      //// Set artifact content if present
      {
        MultipartResponse.Part part = parts.get(RestLockssRepository.MULTIPART_ARTIFACT_PAYLOAD);

        if (part != null) {
          result.setInputStream(part.getInputStream());

          // Set artifact's Content-Type to value of X-Lockss-Content-Type if present,
          // otherwise use value of Content-Type
          HttpHeaders partHeaders = part.getHeaders();
          String contentType = partHeaders.getFirst(ArtifactConstants.X_LOCKSS_CONTENT_TYPE);
          result.getHttpHeaders().set(HttpHeaders.CONTENT_TYPE, StringUtils.isEmpty(contentType) ?
              partHeaders.getFirst(HttpHeaders.CONTENT_TYPE) : contentType);
        }
      }

      return result;

    } catch (IOException e) {
      log.error("Could not process MultipartMessage into ArtifactData object", e);
      throw new IOException("Error processing multipart response");
    }
  }
}
