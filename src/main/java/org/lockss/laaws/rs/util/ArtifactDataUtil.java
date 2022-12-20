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

import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.io.DefaultHttpResponseWriter;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionOutputBufferImpl;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicLineFormatter;
import org.apache.http.util.CharArrayBuffer;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.io.storage.warc.ArtifactState;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Common utilities and adapters for LOCKSS repository ArtifactData objects.
 */
public class ArtifactDataUtil {
    private final static L4JLogger log = L4JLogger.getLogger();

    /**
     * Adapter that takes an {@code ArtifactData} and returns an InputStream containing an HTTP response stream
     * representation of the artifact.
     *
     * @param artifactData
     *          An {@code ArtifactData} to to transform.
     * @return An {@code InputStream} containing an HTTP response stream representation of the artifact.
     * @throws IOException
     * @throws HttpException
     */
    public static InputStream getHttpResponseStreamFromArtifactData(ArtifactData artifactData) throws IOException {
        InputStream httpResponse =
            getHttpResponseStreamFromHttpResponse(getHttpResponseFromArtifactData(artifactData));

	// getBytesRead() hasn't been computed yet
//         artifactData.setContentLength(artifactData.getBytesRead());

        return httpResponse;
    }


    /**
     * Adapter that takes an {@code ArtifactData} and returns an Apache {@code HttpResponse} object representation of
     * the artifact.
     *
     * This is effectively the inverse operation of {@code ArtifactDataFactory#fromHttpResponse(HttpResponse)}.
     *
     * @param artifactData
     *          An {@link ArtifactData} to transform to an HttpResponse object.
     * @return An {@link HttpResponse} object containing a representation of the artifact.
     * @throws HttpException
     * @throws IOException
     */
    public static HttpResponse getHttpResponseFromArtifactData(ArtifactData artifactData) {
        // Craft a new HTTP response object representation from the artifact
        BasicHttpResponse response = new BasicHttpResponse(artifactData.getHttpStatus());

        // Create an InputStreamEntity from artifact InputStream
        response.setEntity(new InputStreamEntity(artifactData.getInputStream()));

        // Add artifact headers into HTTP response
        if (artifactData.getHttpHeaders() != null) {

            // Compile a list of headers
            artifactData.getHttpHeaders().forEach((headerName, headerValues) ->
                headerValues.forEach((headerValue) ->
                    response.addHeader(headerName, headerValue)
            ));
        }

        return response;
    }

    /**
     * Adapter that takes an ArtifactData's ArtifactIdentifier and returns an array of Apache Header objects representing
     * the ArtifactIdentifier.
     *
     * @param artifact
     *          An {@code ArtifactData} whose {@code ArtifactIdentifier} will be adapted.
     * @return An {@code Header[]} representing the {@code ArtifactData}'s {@code ArtifactIdentifier}.
     */
    private static Header[] getArtifactIdentifierHeaders(ArtifactData artifact) {
        return getArtifactIdentifierHeaders(artifact.getIdentifier());
    }

    /**
     * Adapter that takes an {@code ArtifactIdentifier} and returns an array of Apache Header objects representing the
     * ArtifactIdentifier.
     *
     * @param id
     *          An {@code ArtifactIdentifier} to adapt.
     * @return A {@code Header[]} representing the {@code ArtifactIdentifier}.
     */
    private static Header[] getArtifactIdentifierHeaders(ArtifactIdentifier id) {
        Collection<Header> headers = new HashSet<>();
        headers.add(new BasicHeader(ArtifactConstants.ARTIFACT_NAMESPACE_KEY, id.getNamespace()));
        headers.add(new BasicHeader(ArtifactConstants.ARTIFACT_AUID_KEY, id.getAuid()));
        headers.add(new BasicHeader(ArtifactConstants.ARTIFACT_URI_KEY, id.getUri()));
        headers.add(new BasicHeader(ArtifactConstants.ARTIFACT_VERSION_KEY, String.valueOf(id.getVersion())));

        return headers.toArray(new Header[headers.size()]);
    }

    /**
     * Adapts an {@code HttpResponse} object to an InputStream containing a HTTP response stream representation of the
     * {@code HttpResponse} object.
     *
     * @param response
     *          A {@code HttpResponse} to adapt.
     * @return An {@code InputStream} containing a HTTP response stream representation of this {@code HttpResponse}.
     * @throws IOException
     */
    public static InputStream getHttpResponseStreamFromHttpResponse(HttpResponse response) throws IOException {
        // Return the concatenation of the header and content streams
        return new SequenceInputStream(
            new ByteArrayInputStream(getHttpResponseHeader(response)),
            response.getEntity().getContent()
        );
    }

    private static HttpResponse getHttpResponseHeadersFromArtifactData(ArtifactData artifactData) {
        // Craft a new HTTP response object representation from the artifact
        BasicHttpResponse response = new BasicHttpResponse(artifactData.getHttpStatus());

        // Add artifact headers into HTTP response
        if (artifactData.getHttpHeaders() != null) {

            // Compile a list of headers
            artifactData.getHttpHeaders().forEach((headerName, headerValues) ->
                headerValues.forEach((headerValue) ->
                    response.addHeader(headerName, headerValue)
                ));
        }

        return response;
    }

    public static byte[] getHttpResponseHeader(ArtifactData ad) throws IOException {
        return ArtifactDataUtil.getHttpResponseHeader(
            ArtifactDataUtil.getHttpResponseHeadersFromArtifactData(ad));
    }

    public static byte[] getHttpResponseHeader(HttpResponse response) throws IOException {
        try (UnsynchronizedByteArrayOutputStream headerStream = new UnsynchronizedByteArrayOutputStream()) {

            // Create a new SessionOutputBuffer from the OutputStream
            SessionOutputBufferImpl outputBuffer =
                new SessionOutputBufferImpl(new HttpTransportMetricsImpl(), 4096);

            outputBuffer.bind(headerStream);

            // Write the HTTP response header
            writeHttpResponseHeader(response, outputBuffer);

            // Flush anything remaining in the buffer
            outputBuffer.flush();

            return headerStream.toByteArray();
        }
    }

    /**
     * Writes an {@code ArtifactData} to an {@code OutputStream} as a HTTP response stream.
     *
     * @param artifactData
     *          The {@code ArtifactData} to encode as an HTTP response stream and write to an {@code OutputStream}.
     * @param output
     *          The {@code OutputStream} to write to.
     * @throws IOException
     * @throws HttpException
     */
    public static void writeHttpResponseStream(ArtifactData artifactData, OutputStream output) throws IOException {
        writeHttpResponse(
                getHttpResponseFromArtifactData(artifactData),
                output
        );
    }

    /**
     * Writes a HTTP response stream representation of a {@code HttpResponse} to an {@code OutputStream}.
     * @param response
     *          A {@code HttpResponse} to convert to an HTTP response stream and write to the {@code OutputStream}.
     * @param output
     *          The {@code OutputStream} to write to.
     * @throws IOException
     */
    public static void writeHttpResponse(HttpResponse response, OutputStream output) throws IOException {
        // Create a new SessionOutputBuffer from the OutputStream
        SessionOutputBufferImpl outputBuffer = new SessionOutputBufferImpl(new HttpTransportMetricsImpl(),4096);
        outputBuffer.bind(output);

        // Re-construct the response
        writeHttpResponseHeader(response, outputBuffer);
        outputBuffer.flush();
        response.getEntity().writeTo(output);
        output.flush();
    }

    /**
     * Writes a {@code HttpResponse} object's HTTP status and headers to an {@code OutputStream}.
     * @param response
     *          A {@code HttpResponse} whose HTTP status and headers will be written to the {@code OutputStream}.
     * @param outputBuffer
     *          The {@code OutputStream} to write to.
     * @throws IOException
     */
    private static void writeHttpResponseHeader(HttpResponse response, SessionOutputBufferImpl outputBuffer) throws IOException {
        try {
            // Write the HTTP response header
            DefaultHttpResponseWriter responseWriter = new DefaultHttpResponseWriter(outputBuffer);
            responseWriter.write(response);
        } catch (HttpException e) {
            log.error("Caught HttpException while attempting to write the headers of an HttpResponse using DefaultHttpResponseWriter");
            throw new IOException(e);
        }
    }

    public static byte[] getHttpStatusByteArray(StatusLine httpStatus) throws IOException {
        UnsynchronizedByteArrayOutputStream output = new UnsynchronizedByteArrayOutputStream();
        CharArrayBuffer lineBuf = new CharArrayBuffer(128);

        // Create a new SessionOutputBuffer and bind the UnsynchronizedByteArrayOutputStream
        SessionOutputBufferImpl outputBuffer = new SessionOutputBufferImpl(new HttpTransportMetricsImpl(),4096);
        outputBuffer.bind(output);

        // Write HTTP status line
        BasicLineFormatter.INSTANCE.formatStatusLine(lineBuf, httpStatus);
        outputBuffer.writeLine(lineBuf);
        outputBuffer.flush();

        // Flush and close UnsynchronizedByteArrayOutputStream
        output.flush();
        output.close();

        // Return HTTP status byte array
        return output.toByteArray();
    }

  public static MultiValueMap<String, Object> generateMultipartMapFromArtifactData(
      ArtifactData artifactData, LockssRepository.IncludeContent includeContent, long smallContentThreshold)
      throws IOException {

    String artifactUuid = artifactData.getIdentifier().getUuid();

    // Holds multipart response parts
    MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();

    //// Add artifact repository properties multipart
    {
      // Part's headers
      HttpHeaders partHeaders = new HttpHeaders();
      partHeaders.setContentType(MediaType.APPLICATION_JSON);

      // Add repository properties multipart to multiparts list
      parts.add(RestLockssRepository.MULTIPART_ARTIFACT_PROPS,
          new HttpEntity<>(getArtifactProperties(artifactData), partHeaders));
    }

    //// Add HTTP response header multiparts if present
    if (artifactData.isHttpResponse()) {
      //// HTTP status part
      {
        // Part's headers
        HttpHeaders partHeaders = new HttpHeaders();
        partHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        HttpResponse httpResponse = new BasicHttpResponse(artifactData.getHttpStatus());

        httpResponse.setHeaders(
            ArtifactDataFactory.transformHttpHeadersToHeaderArray(artifactData.getHttpHeaders()));

        byte[] header = ArtifactDataUtil.getHttpResponseHeader(httpResponse);

        // Create resource containing HTTP status byte array
        Resource resource = new NamedByteArrayResource(artifactUuid, header);

        // Add artifact headers multipart
        parts.add(RestLockssRepository.MULTIPART_ARTIFACT_HTTP_RESPONSE_HEADER,
            new HttpEntity<>(resource, partHeaders));
      }
    }

    //// Add artifact content part if requested or if small enough
    if ((includeContent == LockssRepository.IncludeContent.ALWAYS) ||
        (includeContent == LockssRepository.IncludeContent.IF_SMALL
            && artifactData.getContentLength() <= smallContentThreshold)) {

      // Create content part headers
      HttpHeaders partHeaders = new HttpHeaders();

      if (artifactData.hasContentLength()) {
        partHeaders.setContentLength(artifactData.getContentLength());
      }

      MediaType type = artifactData.getHttpHeaders().getContentType();
      partHeaders.setContentType(type == null ?
          MediaType.APPLICATION_OCTET_STREAM : type);

      // FIXME: Filename must be set or else Spring will treat the part as a parameter instead of a file
      partHeaders.setContentDispositionFormData(
          RestLockssRepository.MULTIPART_ARTIFACT_PAYLOAD, RestLockssRepository.MULTIPART_ARTIFACT_PAYLOAD);

      // Artifact content
//      InputStreamResource resource = new NamedInputStreamResource(artifactUuid, artifactData.getInputStream());
      InputStreamResource resource = new InputStreamResource(artifactData.getInputStream());

      // Assemble content part and add to multiparts map
      parts.add(RestLockssRepository.MULTIPART_ARTIFACT_PAYLOAD,
          new HttpEntity<>(resource, partHeaders));
    }

    return parts;
  }

  private static Map<String, String> getArtifactProperties(ArtifactData ad) {
    Map<String, String> props = new HashMap<>();
    ArtifactIdentifier id = ad.getIdentifier();

    putIfNotNull(props, Artifact.ARTIFACT_NAMESPACE_KEY, id.getNamespace());
    putIfNotNull(props, Artifact.ARTIFACT_UUID_KEY, id.getUuid());
    props.put(Artifact.ARTIFACT_AUID_KEY, id.getAuid());
    props.put(Artifact.ARTIFACT_URI_KEY, id.getUri());

    Integer version = id.getVersion();
    if (version != null && version > 0) {
      props.put(Artifact.ARTIFACT_VERSION_KEY, String.valueOf(id.getVersion()));
    }

    if (ad.hasContentLength()) {
      props.put(Artifact.ARTIFACT_LENGTH_KEY, String.valueOf(ad.getContentLength()));
    }

    putIfNotNull(props, Artifact.ARTIFACT_DIGEST_KEY, ad.getContentDigest());
    putIfNonZero(props, Artifact.ARTIFACT_COLLECTION_DATE_KEY, ad.getCollectionDate());

    ArtifactState state = ad.getArtifactState();
    if (state != null) {
      props.put(ArtifactState.ARTIFACT_STATE_KEY, String.valueOf(state));
    }

    return props;
  }

  private static void putIfNonZero(Map props, String k, long v) {
    if (v == 0) return;
    props.put(k, String.valueOf(v));
  }

  private static void putIfNotNull(Map props, String k, String v) {
      if (v == null) return;
      props.put(k, v);
  }

  public static Artifact getArtifact(ArtifactData ad) {
    ArtifactState state = ad.getArtifactState();

    Artifact artifact = new Artifact(
        ad.getIdentifier(),
        state != null && state.isCommitted(),
        ad.getStorageUrl() != null ? ad.getStorageUrl().toString() : null,
        ad.getContentLength(),
        ad.getContentDigest());

    artifact.setCollectionDate(ad.getCollectionDate());

    return artifact;
  }
}
