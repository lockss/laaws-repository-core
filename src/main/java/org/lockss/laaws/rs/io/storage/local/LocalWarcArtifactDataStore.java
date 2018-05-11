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

package org.lockss.laaws.rs.io.storage.local;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.*;
import org.apache.commons.logging.*;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Local filesystem implementation of WarcArtifactDataStore.
 */
public class LocalWarcArtifactDataStore extends WarcArtifactDataStore {
    private static final Log log = LogFactory.getLog(LocalWarcArtifactDataStore.class);

    /**
     * Constructor. Rebuilds the index on start-up from a given repository base path, if using a volatile index.
     *
     * @param basePath The base path of the local repository.
     */
    public LocalWarcArtifactDataStore(String basePath) throws IOException {
        super(basePath);
        log.info(String.format("Instantiating a local data store under %s", basePath));

        this.fileAndOffsetStorageUrlPat =
                Pattern.compile("(file://)(" + (getBasePath().equals("/") ? "" : getBasePath()) + ")([^?]+)\\?offset=(\\d+)");

        // Initialize LOCKSS repository structure
        mkdirsIfNeeded(getBasePath());
        mkdirsIfNeeded(getSealedWarcPath());
    }

    /**
     * Recursively finds artifact WARC files under a given base path.
     *
     * @param basePath The base path to scan recursively for WARC files.
     * @return A collection of paths to WARC files under the given base path.
     */
    @Override
    public Collection<String> scanDirectories(String basePath) {
        Collection<String> warcFiles = new ArrayList<>();

        // DFS recursion through directories
        Arrays.stream(new File(basePath).listFiles(x -> x.isDirectory()))
                .map(x -> scanDirectories(x.getPath()))
                .forEach(warcFiles::addAll);

        // Add WARC files from this directory
        warcFiles.addAll(
                Arrays.asList(
                        new File(basePath).listFiles(
                                x -> x.isFile() && x.getName().toLowerCase().endsWith(WARC_FILE_EXTENSION)
                        )
                ).stream().map(File::getPath).collect(Collectors.toSet())
        );

        // Return WARC files at this level
        return warcFiles;
    }

    @Override
    public void mkdirsIfNeeded(String dirPath) throws IOException {
        File dir = new File(getBasePath() + dirPath);
        if (dir.isDirectory()) {
            return;
        }
        if (!dir.mkdirs()) {
            throw new IOException(String.format("Error creating %s: mkdirs did not succeed", dir.getAbsolutePath()));
        }
    }

    @Override
    public long getFileLength(String filePath) {
        return new File(getBasePath() + filePath).length();
    }

    @Override
    public String makeStorageUrl(String filePath, String offset) {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("offset", offset);
        return makeStorageUrl(filePath, params);
    }

    @Override
    public String makeStorageUrl(String filePath, MultiValueMap<String, String> params) {
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + getBasePath() + filePath);
        uriBuilder.queryParams(params);
        return uriBuilder.toUriString();
    }

    @Override
    public OutputStream getAppendableOutputStream(String filePath) throws IOException {
        return new FileOutputStream(getBasePath() + filePath, true);
    }

    @Override
    public InputStream getInputStream(String filePath) throws IOException {
        return new FileInputStream(getBasePath() + filePath);
    }

    @Override
    public InputStream getInputStreamAndSeek(String filePath, long seek) throws IOException {
        InputStream inputStream = getInputStream(filePath);
        inputStream.skip(seek);
        return inputStream;
    }

    @Override
    public InputStream getWarcRecordInputStream(String storageUrl) throws IOException {
        return getFileAndOffsetWarcRecordInputStream(storageUrl);
    }

    @Override
    public void createFileIfNeeded(String filePath) throws IOException {
        File file = new File(getBasePath() + filePath);
        if (!file.exists()) {
            mkdirsIfNeeded(new File(filePath).getParent());
            FileUtils.touch(file);
        }
    }

    @Override
    public void renameFile(String srcPath, String dstPath) throws IOException {
        String realSrcPath = getBasePath() + srcPath;
        String realDstPath = getBasePath() + dstPath;
        if (!new File(realSrcPath).renameTo(new File(realDstPath))) {
            throw new IOException(String.format("Error renaming %s to %s", realSrcPath, realDstPath));
        }
    }

    @Override
    public String makeNewStorageUrl(String newPath, Artifact artifact) {
        return makeNewFileAndOffsetStorageUrl(newPath, artifact);
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as deleted in the repository storage.
     *
     * @param indexData The artifact identifier of the aritfact to check.
     * @return A boolean indicating whether the artifact is marked as deleted.
     * @throws IOException
     * @throws URISyntaxException
     */
    public boolean isDeleted(Artifact indexData)
            throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isDeleted();
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as committed in the repository storage.
     *
     * @param indexData The artifact identifier of the artifact to check.
     * @return A boolean indicating whether the artifact is marked as committed.
     * @throws IOException
     * @throws URISyntaxException
     */
    // TODO this isn't used by anything?
    public boolean isCommitted(Artifact indexData)
            throws IOException, URISyntaxException {
        ArtifactData artifact = getArtifactData(indexData);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isCommitted();
    }

}