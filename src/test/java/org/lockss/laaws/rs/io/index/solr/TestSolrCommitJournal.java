/*
 * Copyright (c) 2021, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.io.index.solr;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;
import org.lockss.util.test.LockssTestCase5;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.lockss.laaws.rs.io.index.solr.SolrCommitJournal.*;
import static org.lockss.laaws.rs.io.index.solr.SolrCommitJournal.SolrOperation.ADD;

public class TestSolrCommitJournal extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static String EMPTY_STRING = "";

  /**
   * Tests for {@link SolrCommitJournal.SolrJournalWriter}.
   */
  @Nested
  class TestSolrJournalWriter {
    /**
     * Test for {@link SolrCommitJournal.SolrJournalWriter#logOperation(String, SolrCommitJournal.SolrOperation, SolrInputDocument)}.
     * @throws Exception
     */
    @Test
    public void testLogOperation() throws Exception {
      ArtifactIdentifier artifactId = new ArtifactIdentifier(
          "test-artifact",
          "test-collection",
          "test-auid",
          "test-url",
          1
      );

      // Create an instance of Artifact to represent the artifact
      Artifact artifact = new Artifact(
          artifactId,
          false,
          "test-storage-url",
          1234L,
          "test-digest"
      );

      // Save the artifact collection date.
      artifact.setCollectionDate(1234L);

      // Convert Artifact to SolrInputDocument using the SolrClient's DocumentObjectBinder
      DocumentObjectBinder binder = new DocumentObjectBinder();
      SolrInputDocument doc = binder.toSolrInputDocument(artifact);

      // Test for ADD
      try (SolrCommitJournal.SolrJournalWriter writer =
          new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        // Write CSV record with logOperation
        writer.logOperation(artifact.getId(), ADD, doc);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readCSVRecords(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_ID));
        assertEquals("ADD", record.get(JOURNAL_HEADER_SOLR_OP));

        String json = "{\n" +
            "  \"id\":{\"id\":\"test-artifact\"},\n" +
            "  \"collection\":{\"collection\":\"test-collection\"},\n" +
            "  \"auid\":{\"auid\":\"test-auid\"},\n" +
            "  \"uri\":{\"uri\":\"test-url\"},\n" +
            "  \"sortUri\":{\"sortUri\":\"test-url\"},\n" +
            "  \"version\":{\"version\":1},\n" +
            "  \"committed\":{\"committed\":false},\n" +
            "  \"storageUrl\":{\"storageUrl\":\"test-storage-url\"},\n" +
            "  \"contentLength\":{\"contentLength\":1234},\n" +
            "  \"contentDigest\":{\"contentDigest\":\"test-digest\"},\n" +
            "  \"collectionDate\":{\"collectionDate\":1234}}";

        assertEquals(json, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }

      // Test for UPDATE (committed)
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        SolrInputDocument commitDoc = new SolrInputDocument();
        commitDoc.addField("id", artifactId.getId());

        Map<String, Object> commitFieldMod = new HashMap<>();
        commitFieldMod.put("set", true);
        commitDoc.addField("committed", commitFieldMod);

        // Write CSV record with logOperation
        writer.logOperation(
            artifact.getId(),
            SolrCommitJournal.SolrOperation.UPDATE,
            commitDoc);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readCSVRecords(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_ID));
        assertEquals("UPDATE", record.get(JOURNAL_HEADER_SOLR_OP));

        String json = "{\n" +
            "  \"id\":{\"id\":\"test-artifact\"},\n" +
            "  \"committed\":{\"committed\":{\"set\":true}}}";

        assertEquals(json, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }

      // Test for DELETE
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        // Write CSV record with logOperation
        writer.logOperation(artifactId.getId(), SolrCommitJournal.SolrOperation.DELETE, null);

        log.info("journal = {}", writer.toString());

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readCSVRecords(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_ID));
        assertEquals("DELETE", record.get(JOURNAL_HEADER_SOLR_OP));
        assertEquals(EMPTY_STRING, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }
    }
  }

  /**
   * Test utility. Reads the journal (a CSV file) and returns the first record in it.
   * @param journalPath A {@link Path} containing the path to the journal.
   * @return A {@link CSVRecord} representing the first CSV record in the file.
   * @throws IOException
   */
  private CSVRecord readCSVRecords(Path journalPath) throws IOException {
    try (FileReader reader = new FileReader(journalPath.toFile())) {
      // Read Solr journal as CSV
      Iterable<CSVRecord> records = CSVFormat.DEFAULT
          .withHeader(SOLR_JOURNAL_HEADERS)
          .withSkipHeaderRecord()
          .parse(reader);

      // Read first CSV record if it exists
      Iterator<CSVRecord> recordIterator = records.iterator();
      return recordIterator.hasNext() ? recordIterator.next() : null;
    }
  }

  /**
   * Tests for {@link SolrCommitJournal.SolrJournalReader}.
   */
  @Nested
  class TestSolrJournalReader {
    /**
     * Test for {@link org.lockss.laaws.rs.io.index.solr.SolrCommitJournal.SolrJournalReader#replaySolrJournal(SolrArtifactIndex)}.
     * @throws Exception
     */
    @Test
    public void testReplaySolrJournal() throws Exception {
      SolrClient solrClient = new MockUpdatableSearchableSolrClient();
      SolrArtifactIndex index = new SolrArtifactIndex(solrClient, "test");

      String headers = "time,artifact,op,doc\n";

      Path ADD = writeTmpFile(headers +
          "1635893660368,test-artifact,ADD,\"{\n" +
          "  \"\"id\"\":{\"\"id\"\":\"\"test-artifact\"\"},\n" +
          "  \"\"collection\"\":{\"\"collection\"\":\"\"test-collection\"\"},\n" +
          "  \"\"auid\"\":{\"\"auid\"\":\"\"test-auid\"\"},\n" +
          "  \"\"uri\"\":{\"\"uri\"\":\"\"test-url\"\"},\n" +
          "  \"\"sortUri\"\":{\"\"sortUri\"\":\"\"test-url\"\"},\n" +
          "  \"\"version\"\":{\"\"version\"\":1},\n" +
          "  \"\"committed\"\":{\"\"committed\"\":false},\n" +
          "  \"\"storageUrl\"\":{\"\"storageUrl\"\":\"\"test-storage-url1\"\"},\n" +
          "  \"\"contentLength\"\":{\"\"contentLength\"\":1234},\n" +
          "  \"\"contentDigest\"\":{\"\"contentDigest\"\":\"\"test-digest\"\"},\n" +
          "  \"\"collectionDate\"\":{\"\"collectionDate\"\":1234}}\"");

      // Test replay of ADD
      try (SolrCommitJournal.SolrJournalReader journalReader
               = new SolrCommitJournal.SolrJournalReader(ADD)) {

        assertFalse(index.artifactExists("test-artifact"));

        // Replay journal
        journalReader.replaySolrJournal(index);

        assertTrue(index.artifactExists("test-artifact"));
        Artifact artifact = index.getArtifact("test-artifact");
        assertFalse(artifact.isCommitted());
        assertEquals("test-storage-url1", artifact.getStorageUrl());
      }

      Path UPDATE_COMMITTED = writeTmpFile(headers +
          "1635893660368,test-artifact,UPDATE,\"{\n" +
          "  \"\"id\"\":{\"\"id\"\":\"\"test-artifact\"\"},\n" +
          "  \"\"committed\"\":{\"\"committed\"\":{\"\"set\"\":true}}}\"");

      // Test replay of UPDATE (committed)
      try (SolrCommitJournal.SolrJournalReader journalReader
               = new SolrCommitJournal.SolrJournalReader(UPDATE_COMMITTED)) {

        // Replay journal
        journalReader.replaySolrJournal(index);

        Artifact artifact = index.getArtifact("test-artifact");
        assertTrue(artifact.isCommitted());
      }

      Path UPDATE_STORAGEURL = writeTmpFile(headers +
          "1635893660368,test-artifact,UPDATE,\"{\n" +
          "  \"\"id\"\":{\"\"id\"\":\"\"test-artifact\"\"},\n" +
          "  \"\"storageUrl\"\":{\"\"storageUrl\"\":{\"\"set\"\":\"\"test-storage-url2\"\"}}}\"");

      // Test replay of UPDATE (storageUrl)
      try (SolrCommitJournal.SolrJournalReader journalReader
               = new SolrCommitJournal.SolrJournalReader(UPDATE_STORAGEURL)) {

        // Replay journal
        journalReader.replaySolrJournal(index);

        Artifact artifact = index.getArtifact("test-artifact");
        assertEquals("test-storage-url2", artifact.getStorageUrl());
      }

      Path DELETE = writeTmpFile(headers +
          "1635893660368,test-artifact,DELETE,\n");

      // Test replay of DELETE
      try (SolrCommitJournal.SolrJournalReader journalReader
               = new SolrCommitJournal.SolrJournalReader(DELETE)) {

        // Replay journal
        journalReader.replaySolrJournal(index);

        assertFalse(index.artifactExists("test-artifact"));
      }
    }

    /**
     * Test utility. Writes a {@link String} to a temporary file and returns its path.
     * @param content
     * @return A {@link Path} containing the path of the temporary file.
     * @throws IOException Thrown if there are any I/O errors.
     */
    private Path writeTmpFile(String content) throws IOException {
      File tmpFile = getTempFile("TestSolrArtifactIndex", null);

      try (FileWriter writer = new FileWriter(tmpFile, false)) {
        writer.write(content);
        writer.write("\n");
      }

      return tmpFile.toPath();
    }
  }
}
