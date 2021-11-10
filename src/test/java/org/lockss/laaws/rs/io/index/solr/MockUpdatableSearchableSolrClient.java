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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.MockSearchableSolrClient;
import org.lockss.log.L4JLogger;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Extends Solr's {@link MockSearchableSolrClient} to support {@link UpdateRequest}s with field modifiers
 * and delete-by-ID.
 */
public class MockUpdatableSearchableSolrClient extends MockSearchableSolrClient {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Number of {@link UpdateRequest}s processed.
   */
  private AtomicLong numUpdates = new AtomicLong();

  /**
   * Overrides {@link MockSearchableSolrClient#request(SolrRequest, String)}. Small portions of this code were copied
   * from the Solr project.
   */
  @Override
  public synchronized NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest request,
                                                String coll) throws SolrServerException, IOException {

    if (coll == null) {
      if (request.getParams() != null) {
        coll = request.getParams().get("collection");
      }
    }
    if (coll == null) {
      coll = "";
    }
    final String collection = coll;
    NamedList<Object> res = new NamedList<>();

    if (request instanceof UpdateRequest) {
      List<String> deleteById = ((UpdateRequest) request).getDeleteById();
      List<SolrInputDocument> docList = ((UpdateRequest) request).getDocuments();

//      if (deleteById != null && docList != null) {
//        log.error("UpdateRequest with both deleteById and SolrInputDocuments is undefined");
//        throw new IllegalArgumentException("Bad UpdateRequest");
//      }

      if (deleteById != null) {

        deleteById.forEach(id ->
            docs.computeIfAbsent(collection, c -> new LinkedHashMap<>()).remove(id));

      } else if (docList != null) {

        docList.forEach(doc -> {
          String id = (String) doc.getFieldValue("id");
          Objects.requireNonNull(id, doc.toString());
          Map<String, SolrInputDocument> map = docs.computeIfAbsent(collection, c -> new LinkedHashMap<>());

          // Does the map for this Solr collection have an SolrInputDocument for this ID?
          if (map.containsKey(id)) {
            // Yes: Update fields based on field modifier

            for (String fieldName : doc.getFieldNames()) {
              SolrInputField field = doc.getField(fieldName);
              Object fieldValue = field.getValue();
              if (fieldValue instanceof Map) {
                // Handle as a field modifier map
                ((Map<String, ?>) fieldValue).entrySet().forEach(entry ->
                    {
                      switch (entry.getKey()) {
                        case "set":
                          SolrInputDocument existingDoc = map.get(id);
                          existingDoc.setField(fieldName, entry.getValue());
                          break;

                        case "add":
                        case "remove":
                        case "removeregex":
                        case "inc":
                          log.debug2("modifier = {}", entry.getKey());
                          throw new UnsupportedOperationException("Unsupported field modifier");

                        default:
                          log.debug2("modifier = {}", entry.getKey());
                          throw new IllegalArgumentException("Unknown field modifier");
                      }
                    }
                );
              } else {
                // Expect existing SolrInputDocument to have matching value for field
                SolrInputDocument existingDoc = map.get(id);
                assert fieldValue.equals(existingDoc.getFieldValue(fieldName));
              }
            }
          } else {
            // No: Put SolrInputDocument as-is into map
            map.put(id, doc);
          }

          numUpdates.incrementAndGet();
        });

      }
    } else {
      // Allow super to process all other types of SolrRequests
      res = super.request(request, coll);
    }

    return res;
  }

  /**
   * Overrides {@link MockSearchableSolrClient#getNumUpdates()}.
   *
   * @return A {@code long} containing the number of {@link UpdateRequest}s processed.
   */
  @Override
  public long getNumUpdates() {
    return numUpdates.get() + super.getNumUpdates();
  }
}
