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

import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.io.*;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;

public class ArtifactUtil {
    private final static Log log = LogFactory.getLog(ArtifactUtil.class);

    public static InputStream getHttpResponseStreamFromArtifact(Artifact artifact) throws IOException, HttpException {
        return getHttpResponseStreamFromHttpResponse(getHttpResponseFromArtifact(artifact));
    }

    // This is the inverse of ArtifactFactory#fromHttpResponse(HttpResponse)
    public static HttpResponse getHttpResponseFromArtifact(Artifact artifact) throws HttpException, IOException {
        // Craft a new HTTP response object representation from the artifact
        BasicHttpResponse response = new BasicHttpResponse(artifact.getHttpStatus());

        // Create an InputStreamEntity from artifact InputStream
        response.setEntity(new InputStreamEntity(artifact.getInputStream()));

        // Merge artifact metadata into HTTP response header
        if (artifact.getMetadata() != null) {
            artifact.getMetadata().forEach((headerName, headerValues) -> {
                headerValues.forEach((headerValue) -> {
                            response.setHeader(headerName, headerValue);
                        }
                );
            });
        }

        // Embed artifact identifier into header if set
        if (artifact.getIdentifier() != null) {
            response.setHeaders(ArtifactUtil.getArtifactIdentifierHeaders(artifact));
        }

        return response;
    }

    private static Header[] getArtifactIdentifierHeaders(Artifact artifact) {
        ArtifactIdentifier id = artifact.getIdentifier();

        Collection<Header> headers = new HashSet<>();
        headers.add(new BasicHeader("X-Lockss-Collection", id.getCollection()));
        headers.add(new BasicHeader("X-Lockss-AuId", id.getAuid()));
        headers.add(new BasicHeader("X-Lockss-Uri", id.getUri()));
        headers.add(new BasicHeader("X-Lockss-Version", id.getVersion()));

        return headers.toArray(new Header[headers.size()]);
    }

    public static InputStream getHttpResponseStreamFromHttpResponse(HttpResponse response) throws IOException {
        // Using DeferredFileOutputStream for this stage over ByteArrayOutputStream is probably overkill but whatever
        DeferredFileOutputStream dfos = new DeferredFileOutputStream(16384, "artifact-dfos", null, new File("/tmp"));

        // Create a new SessionOutputBuffer from the OutputStream
        SessionOutputBufferImpl outputBuffer = new SessionOutputBufferImpl(new HttpTransportMetricsImpl(),4096);
        outputBuffer.bind(dfos);

        // Write the HTTP response header
        writeHttpResponseHeader(response, outputBuffer);

        // Flush anything remaining in the buffer
        outputBuffer.flush();

        // Convert DFOS to InputStream
        InputStream headerStream = dfos.isInMemory() ?
                new ByteArrayInputStream(dfos.getData()) :
                new FileInputStream(dfos.getFile());

        // Return the concatenation of the header and content streams
        return new SequenceInputStream(headerStream, response.getEntity().getContent());
    }

    public static void writeHttpResponseStream(Artifact artifact, OutputStream output) throws IOException, HttpException {
        writeHttpResponse(getHttpResponseFromArtifact(artifact), output);
    }

    public static void writeHttpResponseHeader(HttpResponse response, SessionOutputBufferImpl outputBuffer) throws IOException {
        try {
            // Write the HTTP response header
            DefaultHttpResponseWriter responseWriter = new DefaultHttpResponseWriter(outputBuffer);
            responseWriter.write(response);
        } catch (HttpException e) {
            log.error("Caught HttpException while attempting to write the headers of an HttpResponse using DefaultHttpResponseWriter");
            throw new IOException(e);
        }
    }

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
}
