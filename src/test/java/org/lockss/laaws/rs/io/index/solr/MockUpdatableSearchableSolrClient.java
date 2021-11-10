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

public class MockUpdatableSearchableSolrClient extends MockSearchableSolrClient {
  private final static L4JLogger log = L4JLogger.getLogger();

  private AtomicLong numUpdates = new AtomicLong();

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
      res = super.request(request, coll);
    }

    return res;
  }

  @Override
  public long getNumUpdates() {
    return numUpdates.get() + super.getNumUpdates();
  }
}
