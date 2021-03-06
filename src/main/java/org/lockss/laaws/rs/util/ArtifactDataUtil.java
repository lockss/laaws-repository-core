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
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;

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
        InputStream httpResponse = getHttpResponseStreamFromHttpResponse(
                getHttpResponseFromArtifactData(artifactData)
        );

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
     *          An {@code ArtifactData} to to transform to an HttpResponse object.
     * @return An {@code HttpResponse} object containing a representation of the artifact.
     * @throws HttpException
     * @throws IOException
     */
    public static HttpResponse getHttpResponseFromArtifactData(ArtifactData artifactData) {
        // Craft a new HTTP response object representation from the artifact
        BasicHttpResponse response = new BasicHttpResponse(artifactData.getHttpStatus());

        // Create an InputStreamEntity from artifact InputStream
        response.setEntity(new InputStreamEntity(artifactData.getInputStream()));

        // Add artifact headers into HTTP response
        if (artifactData.getMetadata() != null) {

            // Compile a list of headers
            artifactData.getMetadata().forEach((headerName, headerValues) ->
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
        headers.add(new BasicHeader(ArtifactConstants.ARTIFACT_COLLECTION_KEY, id.getCollection()));
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

    public static byte[] getHttpResponseHeader(HttpResponse response) throws IOException {
        ByteArrayOutputStream headerStream = new ByteArrayOutputStream();

        // Create a new SessionOutputBuffer from the OutputStream
        SessionOutputBufferImpl outputBuffer = new SessionOutputBufferImpl(new HttpTransportMetricsImpl(),4096);
        outputBuffer.bind(headerStream);

        // Write the HTTP response header
        writeHttpResponseHeader(response, outputBuffer);

        // Flush anything remaining in the buffer
        outputBuffer.flush();

        return headerStream.toByteArray();
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
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CharArrayBuffer lineBuf = new CharArrayBuffer(128);

        // Create a new SessionOutputBuffer and bind the ByteArrayOutputStream
        SessionOutputBufferImpl outputBuffer = new SessionOutputBufferImpl(new HttpTransportMetricsImpl(),4096);
        outputBuffer.bind(output);

        // Write HTTP status line
        BasicLineFormatter.INSTANCE.formatStatusLine(lineBuf, httpStatus);
        outputBuffer.writeLine(lineBuf);
        outputBuffer.flush();

        // Flush and close ByteArrayOutputStream
        output.flush();
        output.close();

        // Return HTTP status byte array
        return output.toByteArray();
    }
}
