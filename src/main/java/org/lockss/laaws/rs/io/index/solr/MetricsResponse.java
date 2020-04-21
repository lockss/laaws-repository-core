package org.lockss.laaws.rs.io.index.solr;

import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.lockss.log.L4JLogger;

public class MetricsResponse extends SolrResponseBase {
  private final static L4JLogger log = L4JLogger.getLogger();

  public static class CoreMetricsResponse extends MetricsResponse {
    public long getTotalSpace() {
      return (long) getResponse().findRecursive("metrics", "solr.core.lockss-repo", "CORE.fs.totalSpace");
    }

    public long getUsableSpace() {
      return (long) getResponse().findRecursive("metrics", "solr.core.lockss-repo", "CORE.fs.usableSpace");
    }

    public long getIndexSizeInBytes() {
      return (long) getResponse().findRecursive("metrics", "solr.core.lockss-repo", "INDEX.sizeInBytes");
    }

    public String getIndexDir() {
      return (String) getResponse().findRecursive("metrics", "solr.core.lockss-repo", "CORE.indexDir");
    }
  }

  public static class NodeMetricsResponse extends MetricsResponse {
    public long getTotalSpace() {
      return (long) getResponse().findRecursive("metrics", "solr.node", "CONTAINER.fs.totalSpace");
    }

    public long getUsableSpace() {
      return (long) getResponse().findRecursive("metrics", "solr.node", "CONTAINER.fs.usableSpace");
    }
  }
}
