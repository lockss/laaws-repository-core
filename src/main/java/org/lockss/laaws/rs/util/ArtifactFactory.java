/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.*;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.entity.LaxContentLengthStrategy;
import org.apache.http.impl.io.*;
import org.apache.http.message.BasicStatusLine;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;

/**
 * Artifact factory: Instantiates Artifact objects from a variety of sources.
 */
public class ArtifactFactory {
    private final static Log log = LogFactory.getLog(ArtifactFactory.class);

    private static final String RESPONSE_TYPE = WARCConstants.WARCRecordType.response.toString();
    private static final String RESOURCE_TYPE = WARCConstants.WARCRecordType.resource.toString();

    /**
     * Instantiates an {@code Artifact} from an {@code InputStream} containing the byte stream of an HTTP response.
     *
     * @param responseStream
     *          An {@code InputStream} containing an HTTP response byte stream which in turn encodes an artifact.
     * @return An {@code Artifact} representing the artifact encoded in an HTTP response input stream.
     * @throws IOException
     */
    public static Artifact fromHttpResponseStream(InputStream responseStream) throws IOException {
        return fromHttpResponseStream(null, responseStream);
    }

    /**
     * Instantiates an {@code Artifact} from an {@code InputStream} containing the byte stream of an HTTP response.
     *
     * Allows additional HTTP headers to be injected by passing a {@code HttpHeaders}.
     *
     * @param additionalMetadata
     *          A {@code HttpHeader} with additional headers.
     * @param responseStream
     *          An {@code InputStream} containing an HTTP response byte stream which in turn encodes an artifact.
     * @return An {@code Artifact} representing the artifact encoded in an HTTP response input stream.
     * @throws IOException
     */
    public static Artifact fromHttpResponseStream(HttpHeaders additionalMetadata, InputStream responseStream)
            throws IOException
    {
        // Create a SessionInputBuffer from the InputStream containing a HTTP response
        SessionInputBufferImpl buffer = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 4096);
        buffer.bind(responseStream);

        // Attach remaining data in stream as the response entity. We cannot use InputStreamEntity directly because
        // it is now wrapped within a SessionInputBufferImpl so we instantiate a BasicHttpEntity and populate it an
        // IdentityInputStream. We could also have used StrictContentLengthStrategy and ContentLengthInputStream.
        try {
            BasicHttpEntity responseEntity = new BasicHttpEntity();
            HttpResponse response = (new DefaultHttpResponseParser(buffer)).parse();
            long len = (new LaxContentLengthStrategy()).determineLength(response);
            responseEntity.setContentLength(len);
            responseEntity.setContent(new IdentityInputStream(buffer));
            response.setEntity(responseEntity);

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
     * Instantiates an {@code Artifact} from a Apache {@code HttpResponse} object.
     *
     * @param response
     *          A {@code HttpResponse} object containing an artifact.
     * @return An {@code Artifact} representing the artifact encoded in the {@code HttpResponse} object.
     * @throws IOException
     */
    public static Artifact fromHttpResponse(HttpResponse response) throws IOException {
        HttpHeaders headers = transformHeaderArrayToHttpHeaders(response.getAllHeaders());

        return new Artifact(
                buildArtifactIdentifier(headers),
                headers,
                response.getEntity().getContent(),
                response.getStatusLine()
        );
    }

    /**
     * Instantiates an {@code ArtifactIdentifier} from HTTP headers in a {@code HttpHeaders} object.
     *
     * @param headers
     *          An {@code HttpHeaders} object representing HTTP headers containing an artifact identity.
     * @return An {@code ArtifactIdentifier}.
     */
    private static ArtifactIdentifier buildArtifactIdentifier(HttpHeaders headers) {
        return new ArtifactIdentifier(
                getHeaderValue(headers, ArtifactConstants.ARTIFACTID_ID_KEY),
                getHeaderValue(headers, ArtifactConstants.ARTIFACTID_COLLECTION_KEY),
                getHeaderValue(headers, ArtifactConstants.ARTIFACTID_AUID_KEY),
                getHeaderValue(headers, ArtifactConstants.ARTIFACTID_URI_KEY),
                getHeaderValue(headers, ArtifactConstants.ARTIFACTID_VERSION_KEY)
        );
    }

    /**
     * Instantiates an {@code ArtifactIdentifier} from headers in a ARC / WARC {@code ArchiveRecordHeader} object.
     *
     * @param headers
     *          An {@code ArchiveRecordHeader} ARC / WARC header containing an artifact identity.
     * @return An {@code ArtifactIdentifier}.
     */
   private static ArtifactIdentifier buildArtifactIdentifier(ArchiveRecordHeader headers) {
        return new ArtifactIdentifier(
                (String)headers.getHeaderValue(ArtifactConstants.ARTIFACTID_ID_KEY),
//                (String)headers.getHeaderValue(WARCConstants.HEADER_KEY_ID),
                (String)headers.getHeaderValue(ArtifactConstants.ARTIFACTID_COLLECTION_KEY),
                (String)headers.getHeaderValue(ArtifactConstants.ARTIFACTID_AUID_KEY),
                (String)headers.getHeaderValue(ArtifactConstants.ARTIFACTID_URI_KEY),
//                (String)headers.getHeaderValue(WARCConstants.HEADER_KEY_URI),
                (String)headers.getHeaderValue(ArtifactConstants.ARTIFACTID_VERSION_KEY)
        );
    }

    /**
     * Returns the value from an {@code HttpHeaders} object for a given key.
     *
     * The value must for this key must be unique.
     *
     * @param headers
     *          A {@code HttpHeaders} to return the key's value from.
     * @param key
     *          A {@code String} containing the key of the value to return.
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
     * @param headerArray
     *          An array of {@code Header} objects to reorganize.
     * @return A Spring {@code HttpHeaders} object representing the array of Apache {@code Header} objects.
     */
    private static HttpHeaders transformHeaderArrayToHttpHeaders(Header[] headerArray) {
        HttpHeaders headers = new HttpHeaders();
        for (Header header : headerArray)
            headers.add(header.getName(), header.getValue());

        //Stream.of(headerArray).forEach(header -> headers.add(header.getName(), header.getValue()));
        return headers;
    }

    /**
     * Instantiates an {@code Artifact} from an arbitrary byte stream in an {@code InputStream}.
     *
     * @param resourceStream
     *          An {@code InputStream} containing the byte stream to encode into an {@code Artifact}.
     * @return An {@code Artifact} wrapping the byte stream.
     */
    public static Artifact fromResource(InputStream resourceStream) {
        return fromResourceStream(null, resourceStream);
    }

    /**
     * Instantiates an {@code Artifact} from an arbitrary byte stream in an {@code InputStream}.
     *
     * Uses a default HTTP response status of HTTP/1.1 200 OK.
     *
     * @param metadata
     *          A Spring {@code HttpHeaders} object containing optional artifact headers.
     * @param resourceStream
     *          An {@code InputStream} containing an arbitrary byte stream.
     * @return An {@code Artifact} wrapping the byte stream.
     */
    public static Artifact fromResourceStream(HttpHeaders metadata, InputStream resourceStream) {
        StatusLine responseStatus = new BasicStatusLine(
                new ProtocolVersion("HTTP", 1, 1),
                200,
                "OK"
        );

        return fromResourceStream(metadata, resourceStream, responseStatus);
    }

    /**
     * Instantiates an {@code Artifact} from an arbitrary byte stream in an {@code InputStream}.
     *
     * Takes a {@code StatusLine} with the HTTP response status associated with this byte stream.
     *
     * @param metadata
     *          A Spring {@code HttpHeaders} object containing optional artifact headers.
     * @param resourceStream
     *          An {@code InputStream} containing an arbitrary byte stream.
     * @param responseStatus
     * @return An {@code Artifact} wrapping the byte stream.
     */
    public static Artifact fromResourceStream(HttpHeaders metadata, InputStream resourceStream, StatusLine responseStatus) {
        return new Artifact(metadata, resourceStream, responseStatus);
    }

    /**
     * Instantiates an {@code Artifact} from an ARC / WARC {@code ArchiveRecord} object containing an artifact.
     *
     * @param record
     *          An {@code ArchiveRecord} object containing an artifact.
     * @return An {@code Artifact} representing the artifact contained in the {@code ArchiveRecord}.
     * @throws IOException
     */
    public static Artifact fromArchiveRecord(ArchiveRecord record) throws IOException {
        // Get WARC record header
        ArchiveRecordHeader headers = record.getHeader();
        String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

        // Create an ArtifactIdentifier object from the WARC record headers
        ArtifactIdentifier artifactId = buildArtifactIdentifier(headers);

        // Artifacts can only be read out of WARC response and resource type records
        if (recordType.equals(RESPONSE_TYPE)) {
            if (!headers.getMimetype().startsWith("application/http")) {
                log.warn(String.format(
                        "Unexpected content MIME type (%s) from a WARC response record",
                        headers.getMimetype()
                ));

                return null;
            }

            // Parse the ArchiveRecord into an artifact and return it
            Artifact artifact = ArtifactFactory.fromHttpResponseStream(record);
            artifact.setIdentifier(artifactId);
            return artifact;

        } else if (recordType.equals(RESOURCE_TYPE)) {
            // Holds optional HTTP headers for metadata
            HttpHeaders metadata = new HttpHeaders();

            // Setup artifact headers from WARC record headers
            metadata.setContentLength((int) headers.getContentLength());
            metadata.setContentType(MediaType.valueOf(headers.getMimetype()));

            // Use WARC-Date as our notion of crawl/capture/collecton time in this context
            TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(headers.getDate());
            metadata.setDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toEpochSecond());
            //metadata.setDate(DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)));
            //metadata.setDate(DateTimeFormatter.RFC_1123_DATE_TIME.format(
            //        ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC)
            //));

            // Custom header to indicate the origin of this artifact
            metadata.add("X-Lockss-Origin", "warc");

            // Parse the ArchiveRecord into an artifact and return it
            return ArtifactFactory.fromResourceStream(metadata, record);

        } else {
            log.warn(String.format(
                    "Skipped WARC record %s of type %s; artifacts can only be created from response or resource types",
                    record.getHeader().getHeaderValue("WARC-Record-ID"),
                    recordType
            ));
        }

        // Could not return an artifact elsewhere
        return null;
    }
}
