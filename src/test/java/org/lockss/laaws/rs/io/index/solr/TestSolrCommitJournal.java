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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.log.L4JLogger;
import org.lockss.util.ListUtil;
import org.lockss.util.test.LockssTestCase5;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.lockss.laaws.rs.io.index.solr.SolrCommitJournal.*;
import static org.lockss.laaws.rs.io.index.solr.SolrCommitJournal.SolrOperation.ADD;
import static org.mockito.Mockito.*;

public class TestSolrCommitJournal extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static String EMPTY_STRING = "";

  protected boolean wantTempTmpDir() {
    return true;
  }

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
          "test-namespace",
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
        writer.logOperation(artifact.getUuid(), ADD, doc);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("ADD", record.get(JOURNAL_HEADER_SOLR_OP));

        String json = "{\n" +
            "  \"id\":\"test-artifact\",\n" +
            "  \"namespace\":\"test-namespace\",\n" +
            "  \"auid\":\"test-auid\",\n" +
            "  \"uri\":\"test-url\",\n" +
            "  \"sortUri\":\"test-url\",\n" +
            "  \"version\":1,\n" +
            "  \"committed\":false,\n" +
            "  \"storageUrl\":\"test-storage-url\",\n" +
            "  \"contentLength\":1234,\n" +
            "  \"contentDigest\":\"test-digest\",\n" +
            "  \"collectionDate\":1234}";

        assertEquals(json, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }

      // Test for UPDATE (committed)
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        SolrInputDocument updateSolrDoc = new SolrInputDocument();
        updateSolrDoc.addField("uuid", artifactId.getUuid());

        Map<String, Object> commitFieldMod = new HashMap<>();
        commitFieldMod.put("set", true);
        updateSolrDoc.addField("committed", commitFieldMod);

        // Write CSV record with logOperation
        writer.logOperation(
            artifact.getUuid(),
            SolrCommitJournal.SolrOperation.UPDATE,
            updateSolrDoc);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("UPDATE", record.get(JOURNAL_HEADER_SOLR_OP));

        String json = "{\n" +
            "  \"uuid\":\"test-artifact\",\n" +
            "  \"committed\":{\"set\":true}}";

        assertEquals(json, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }

      // Test for UPDATE (storage URL)
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        SolrInputDocument updateSolrDoc = new SolrInputDocument();
        updateSolrDoc.addField("uuid", artifactId.getUuid());

        Map<String, Object> storageUrlFieldMod = new HashMap<>();
        storageUrlFieldMod.put("set", "test-storage-url2");
        updateSolrDoc.addField("storageUrl", storageUrlFieldMod);

        // Write CSV record with logOperation
        writer.logOperation(
            artifact.getUuid(),
            SolrCommitJournal.SolrOperation.UPDATE,
            updateSolrDoc);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("UPDATE", record.get(JOURNAL_HEADER_SOLR_OP));

        String json = "{\n" +
            "  \"uuid\":\"test-artifact\",\n" +
            "  \"storageUrl\":{\"set\":\"test-storage-url2\"}}";

        assertEquals(json, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }

      // Test for DELETE
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        // Write CSV record with logOperation
        writer.logOperation(artifactId.getUuid(), SolrCommitJournal.SolrOperation.DELETE, null);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("DELETE", record.get(JOURNAL_HEADER_SOLR_OP));
        assertEquals(EMPTY_STRING, record.get(JOURNAL_HEADER_INPUT_DOCUMENT));
      }
    }

    /**
     * Test for {@link SolrJournalWriter#renameWithSuffix(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testRenameWithSuffix() throws Exception {
      File journalFile = getTempFile("journal-test", null);
      Path journalPath = journalFile.toPath();
      String SUFFIX = "test";

      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(journalFile.toPath())) {

        writer.renameWithSuffix(SUFFIX);
        File renamedJournalFile = writer.getJournalPath().toFile();

        Path expectedPath = journalPath
            .resolveSibling(journalPath.getFileName() + "." + SUFFIX);

        assertFalse(journalFile.exists());
        assertEquals(expectedPath, writer.getJournalPath());
        assertTrue(renamedJournalFile.exists());
        assertTrue(renamedJournalFile.isFile());
      }
    }
  }

  /**
   * Test utility. Reads the journal (a CSV file) and returns the first record in it.
   * @param journalPath A {@link Path} containing the path to the journal.
   * @return A {@link CSVRecord} representing the first CSV record in the file.
   * @throws IOException
   */
  private CSVRecord readFirstCSVRecord(Path journalPath) throws IOException {
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

  interface Assertable<T> {
    void runAssert(T input);
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
      testReplaySolrJournal_ADD();
      testReplaySolrJournal_UPDATE_COMMITTED();
      testReplaySolrJournal_UPDATE_STORAGEURL();
      testReplaySolrJournal_DELETE();
    }

    private final String CSV_HEADERS = "time,artifact,op,doc\n";

    private void testReplaySolrJournal_ADD() throws Exception {
      ArtifactIdentifier ADD_ARTIFACTID = new ArtifactIdentifier(
          "test-artifact",
          "test-namespace",
          "test-auid",
          "test-url",
          1
      );

      // Create an instance of Artifact to represent the artifact
      Artifact ADD_ARTIFACT = new Artifact(
          ADD_ARTIFACTID,
          false,
          "test-storage-url1",
          1234,
          "test-digest"
      );

      // Save the artifact collection date.
      ADD_ARTIFACT.setCollectionDate(1234L);

      // Convert Artifact to SolrInputDocument using the SolrClient's DocumentObjectBinder
      DocumentObjectBinder binder = new DocumentObjectBinder();
      SolrInputDocument ADD_DOC = binder.toSolrInputDocument(ADD_ARTIFACT);

      Path ADD_FILE = writeTmpFile(CSV_HEADERS + "1636690761743,test-artifact,ADD,\"{\n" +
          "  \"\"uuid\"\":\"\"test-artifact\"\",\n" +
          "  \"\"namespace\"\":\"\"test-namespace\"\",\n" +
          "  \"\"auid\"\":\"\"test-auid\"\",\n" +
          "  \"\"uri\"\":\"\"test-url\"\",\n" +
          "  \"\"sortUri\"\":\"\"test-url\"\",\n" +
          "  \"\"version\"\":1,\n" +
          "  \"\"committed\"\":false,\n" +
          "  \"\"storageUrl\"\":\"\"test-storage-url1\"\",\n" +
          "  \"\"contentLength\"\":1234,\n" +
          "  \"\"contentDigest\"\":\"\"test-digest\"\",\n" +
          "  \"\"collectionDate\"\":1234}\"");

      runTestReplaySolrJournal(ADD_FILE, (updateRequest) -> {
        // Assert replayed UpdateRequest
        List<SolrInputDocument> docs = updateRequest.getDocuments();
        assertEquals(1, docs.size());
        SolrInputDocument doc = docs.get(0);
        assertSolrInputDocumentEquals(ADD_DOC, doc);
      });
    }

    private void testReplaySolrJournal_UPDATE_COMMITTED() throws Exception {
      // Write journal with CSV record to temp file
      Path UPDATE_COMMITTED = writeTmpFile(CSV_HEADERS +
          "1636692345004,test-artifact,UPDATE,\"{\n" +
          "  \"\"id\"\":\"\"test-artifact\"\",\n" +
          "  \"\"committed\"\":{\"\"set\"\":true}}\"");

      // Construct expected SolrInputDocument for UPDATE_STORAGEURL
      SolrInputDocument EXPECTED_UPDATE_COMMITTED_DOC = new SolrInputDocument();
      EXPECTED_UPDATE_COMMITTED_DOC.addField("id", "test-artifact");
      Map<String, Object> fm1 = new HashMap<>();
      fm1.put("set", true);
      EXPECTED_UPDATE_COMMITTED_DOC.addField("committed", fm1);

      runTestReplaySolrJournal(UPDATE_COMMITTED, updateRequest -> {
        List<SolrInputDocument> docs = updateRequest.getDocuments();
        assertEquals(1, docs.size());
        SolrInputDocument doc = docs.get(0);
        assertSolrInputDocumentEquals(EXPECTED_UPDATE_COMMITTED_DOC, doc);
      });
    }

    private void testReplaySolrJournal_UPDATE_STORAGEURL() throws Exception {
      // Write journal with CSV record to temp file
      Path UPDATE_STORAGEURL = writeTmpFile(CSV_HEADERS +
          "1636692454871,test-artifact,UPDATE,\"{\n" +
          "  \"\"id\"\":\"\"test-artifact\"\",\n" +
          "  \"\"storageUrl\"\":{\"\"set\"\":\"\"test-storage-url2\"\"}}\"");

      // Construct expected SolrInputDocument for UPDATE_STORAGEURL
      SolrInputDocument EXPECTED_UPDATE_STORAGEAURL_DOC = new SolrInputDocument();
      EXPECTED_UPDATE_STORAGEAURL_DOC.addField("id", "test-artifact");
      Map<String, Object> fm2 = new HashMap<>();
      fm2.put("set", "test-storage-url2");
      EXPECTED_UPDATE_STORAGEAURL_DOC.addField("storageUrl", fm2);

      runTestReplaySolrJournal(UPDATE_STORAGEURL, updateRequest -> {
        List<SolrInputDocument> docs = updateRequest.getDocuments();
        assertEquals(1, docs.size());
        SolrInputDocument doc = docs.get(0);
        assertSolrInputDocumentEquals(EXPECTED_UPDATE_STORAGEAURL_DOC, doc);
      });
    }

    private void testReplaySolrJournal_DELETE() throws Exception {
      Path DELETE = writeTmpFile(CSV_HEADERS +
          "1635893660368,test-artifact,DELETE,\n");

      runTestReplaySolrJournal(DELETE, updateRequest -> {
        List<String> deleteById = updateRequest.getDeleteById();
        assertIterableEquals(ListUtil.list("test-artifact"), deleteById);
      });
    }

    private void runTestReplaySolrJournal(Path journalPath, Assertable<UpdateRequest> assertable) throws Exception {
      // Setup mock SolrClient
      SolrClient solrClient = mock(SolrClient.class);
      when(solrClient.request(ArgumentMatchers.any(SolrRequest.class), ArgumentMatchers.anyString()))
          .thenReturn(mock(NamedList.class));

      // Create new SolrArtifactIndex using mocked SolrClient
      SolrArtifactIndex index = new SolrArtifactIndex(solrClient, "test-solr-collection");

      // Test replay of UPDATE (committed)
      try (SolrCommitJournal.SolrJournalReader journalReader
               = new SolrCommitJournal.SolrJournalReader(journalPath)) {

        // Replay journal
        journalReader.replaySolrJournal(index);

        ArgumentCaptor<SolrRequest> requests = ArgumentCaptor.forClass(SolrRequest.class);
        ArgumentCaptor<String> collections = ArgumentCaptor.forClass(String.class);

        verify(solrClient, times(3))
            .request(requests.capture(), collections.capture());

        // Verify all SolrRequests made were of type UpdateRequest
        assertTrue(requests.getAllValues().stream()
            .allMatch(request -> request instanceof UpdateRequest));

        // Verify all UpdateRequests were performed on the expected Solr collection
        assertTrue(collections.getAllValues().stream()
            .allMatch(collection -> collection.equals("test-solr-collection")));

        // Assert replayed UpdateRequest
        UpdateRequest r0 = (UpdateRequest) requests.getAllValues().get(0);
        assertable.runAssert(r0);

        // Assert soft commit
        UpdateRequest r1 = (UpdateRequest) requests.getAllValues().get(1);
        assertEquals("true", r1.getParams().get("commit"));
        assertEquals("true", r1.getParams().get("softCommit"));
        assertEquals("true", r1.getParams().get("waitSearcher"));

        // Assert hard commit
        UpdateRequest r2 = (UpdateRequest) requests.getAllValues().get(2);
        assertEquals("true", r2.getParams().get("commit"));
        assertEquals("false", r2.getParams().get("softCommit"));
        assertEquals("true", r2.getParams().get("waitSearcher"));

        // Reset mock
        clearInvocations(solrClient);
      }
    }

    private void assertSolrInputDocumentEquals(SolrInputDocument doc1, SolrInputDocument doc2) {
      // Assert both SolrInputDocuments contain the same field names
      assertIterableEquals(doc1.getFieldNames(), doc2.getFieldNames());

      // Assert the field values are the same
      for (String fieldName : doc1.getFieldNames()) {
        Object f1 = doc1.getFieldValue(fieldName);
        Object f2 = doc2.getFieldValue(fieldName);
        assertEquals(f1, f2);
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
