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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.Utils;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;

public class SolrCommitJournal {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * CSV headers used for the journal of changes made to the Solr index.
   */
  static final String JOURNAL_HEADER_TIME = "time";
  static final String JOURNAL_HEADER_ARTIFACT_ID = "artifact";
  static final String JOURNAL_HEADER_SOLR_OP = "op";
  static final String JOURNAL_HEADER_INPUT_DOCUMENT = "doc";

  /**
   * Array of CSV headers. Used with {@link CSVPrinter}.
   */
  static final String[] SOLR_JOURNAL_HEADERS = {
      JOURNAL_HEADER_TIME,
      JOURNAL_HEADER_ARTIFACT_ID,
      JOURNAL_HEADER_SOLR_OP,
      JOURNAL_HEADER_INPUT_DOCUMENT
  };

  /**
   * Explicit empty string.
   */
  private static final String EMPTY_STRING = "";

  /**
   * Types of Solr updates.
   */
  public enum SolrOperation {
    ADD, UPDATE, DELETE
  }

  /**
   * Logs Solr operations into a CSV journal of changes to Solr.
   */
  public static class SolrJournalWriter implements Closeable {
    private Path journalPath;
    private BufferedWriter journalFileWriter;
    private CSVPrinter journalPrinter;

    public SolrJournalWriter(Path journalPath) throws IOException {
      this.journalPath = journalPath;

      journalFileWriter = Files.newBufferedWriter(
          journalPath,
          StandardOpenOption.APPEND,
          StandardOpenOption.CREATE);

      journalPrinter = new CSVPrinter(journalFileWriter, CSVFormat.DEFAULT
          .withHeader(SOLR_JOURNAL_HEADERS)
          .withSkipHeaderRecord(false));
    }

    @Override
    public void close() throws IOException {
      journalPrinter.close();
      journalFileWriter.close();
    }

    public void rename(Path name) {
      Path p = journalPath.getParent();
    }

//    @Override
//    public String toString() {
//      StringBuilder builder = new StringBuilder();
//
//      try (BufferedReader reader = Files.newBufferedReader(journalPath)) {
//        journalFileWriter.flush();
//        reader.lines()
//            .map(line -> line + "\n")
//            .forEach(builder::append);
//      } catch (IOException e) {
//        log.error("Could not read journal file", e);
//      }
//
//      return builder.toString();
//    }

    public void logOperation(String artifactId, SolrOperation op, SolrInputDocument doc) throws IOException {
      try {
        // Transform SolrInputDocument to JSON (if one was provided)
        String docJson = (doc == null) ? EMPTY_STRING : toJSON(doc);

        // Write journal entry (i.e., CSV record)
        journalPrinter.printRecord(System.currentTimeMillis(), artifactId, op, docJson);
        journalPrinter.flush();
      } catch (IOException e) {
        log.error("Could not write to Solr journal", e);
        throw e;
      }
    }

    public Path getJournalPath() {
      return this.journalPath;
    }
  }

  public static class SolrJournalReader implements Closeable {
    private final Path journalPath;
    private final ObjectMapper mapper;

    public SolrJournalReader(Path journalPath) {
      this.journalPath = journalPath;
      this.binder =  new DocumentObjectBinder();
      this.mapper = new ObjectMapper();
//      this.mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Replays Solr operations from a journal of operations performed since the last hard commit.
     *
     * @param index The {@link SolrArtifactIndex} instance to replay the journal to.
     * @throws IOException
     */
    public void replaySolrJournal(SolrArtifactIndex index) throws IOException {
      try (FileReader reader = new FileReader(journalPath.toFile())) {
        // Read Solr journal as CSV
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
            .withHeader(SOLR_JOURNAL_HEADERS)
            .withSkipHeaderRecord()
            .parse(reader);

        // Replay Solr operation represented by CSV row
        records.forEach(record -> {
          try {
            // Determine Solr operation to replay
            SolrOperation op = SolrOperation.valueOf(record.get(JOURNAL_HEADER_SOLR_OP));

            log.debug("Replaying journal entry [op: {}, artifactId: {}]",
                op, record.get(JOURNAL_HEADER_ARTIFACT_ID));

            // Replay Solr operation
            switch (op) {
              case ADD:
              {
                // Transform JSON to SolrInputDocument
                Artifact artifact = mapper.readValue(record.get(JOURNAL_HEADER_INPUT_DOCUMENT), Artifact.class);
                SolrInputDocument doc = binder.toSolrInputDocument(artifact);

                UpdateRequest req = new UpdateRequest();
                req.add(doc);
                processUpdateRequest(index, req);
                return;
              }

              case UPDATE: {
                // Transform JSON to SolrInputDocument
                SolrInputDocument doc = transformMapToSolrInputDocument(
                    mapper.readValue(record.get(JOURNAL_HEADER_INPUT_DOCUMENT), Map.class));

                UpdateRequest req = new UpdateRequest();
                req.add(doc);
                processUpdateRequest(index, req);
                return;
              }

              case DELETE: {
                UpdateRequest req = new UpdateRequest();
                req.deleteById(record.get(JOURNAL_HEADER_ARTIFACT_ID));
                processUpdateRequest(index, req);
                return;
              }

              default:
                log.error("Unknown Solr operation [op: {}, record: {}]", op, record);
            }
          } catch (IOException | SolrServerException | SolrResponseErrorException e) {
            log.error("Could not replay journal entry", e);
          }
        });

        // Perform a Solr hard commit of all changes
        try {
          index.handleSolrResponse(index.handleSolrCommit(true), "Error with Commit request");
        } catch (IOException | SolrServerException | SolrResponseErrorException e) {
          log.error("Could not perform a Solr hard commit after replaying journal", e);
        }
      }
    }

    static private void processUpdateRequest(SolrArtifactIndex index, UpdateRequest req) throws SolrServerException, IOException, SolrResponseErrorException {
      index.addSolrCredentials(req);

      index.handleSolrResponse(
          req.process(index.getSolrClient(), index.getSolrCollection()), "Error with UpdateRequest");

      index.handleSolrResponse(
          index.handleSolrCommit(false), "Error with Commit request");
    }
  }

  /**
   * Copied from SolrJ's {@link Utils#toJSON(Object)}. Uses our {@link FixedJSONWriter} and returns a {@link String}
   * rather than a {@code byte[]}.
   *
   * @param o The {@link Object} to serialize to JSON.
   * @return A {@link String} containing the JSON serialization of the {@link Object}.
   */
  private static String toJSON(Object o) {
    if (o == null) return EMPTY_STRING;
    CharArr out = new CharArr();
    new FixedJSONWriter(out, 2).write(o);
    return out.toString();
  }

  /**
   * Extends SolrJ's {@link JSONWriter} to support serializing {@link SolrInputField} objects to JSON.
   */
  private static class FixedJSONWriter extends JSONWriter {
    public FixedJSONWriter(CharArr out, int indentSize) {
      super(out, indentSize);
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public void handleUnknownClass(Object o) {
      if (o instanceof SolrInputField) {
        SolrInputField field = (SolrInputField)o;
        if (field.getValueCount() > 1) {
          // Yes: Multivalued field; write array of values
          startArray();
          Iterator<Object> iter = field.getValues().iterator();

          while (true) {
            write(iter.next());
            if (!iter.hasNext()) break;
            writeValueSeparator();
          }

          endArray();
        } else {
          // No: Single valued field
          write(field.getValue());
        }
      } else {
        super.handleUnknownClass(o);
      }
    }
  }

  /**
   * Transforms a {@link Map} into a {@link SolrInputDocument} containing {@link SolrInputField}s.
   * Used for Solr journal replay. This method is necessary because {@link Utils#fromJSON(byte[])}
   * doesn't transform JSON into a {@link SolrInputDocument} with {@link SolrInputField}s.
   *
   * @param docMap The {@link Map} to transform.
   * @return A {@link SolrInputDocument} representing the original {@link Map}.
   */
  private static SolrInputDocument transformMapToSolrInputDocument(Map<String, Object> docMap) {
    SolrInputDocument doc = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : docMap.entrySet()) {
      doc.setField(entry.getKey(), entry.getValue());
    }

    return doc;
  }
}
