/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.lockss.log.L4JLogger;

public abstract class MetricsRequest<T extends MetricsResponse> extends SolrRequest<T> {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Metrics API query parameter keys
   */
  public static final String METRICS_PARAM_GROUP = "group";
  public static final String METRICS_PARAM_TYPE = "type";
  public static final String METRICS_PARAM_PREFIX = "prefix";
  public static final String METRICS_PARAM_REGEX = "regex";
  public static final String METRICS_PARAM_PROPERTY = "property";
  public static final String METRICS_PARAM_KEY = "key";

  public enum MetricsGroups {
    all,
    jvm,
    jetty,
    node,
    core
  }

  public enum MetricsTypes {
    all,
    counter,
    gauge,
    histogram,
    meter,
    timer
  }

  /**
   * Metrics API query parameters.
   */
  protected MetricsGroups group = MetricsGroups.all;
  protected MetricsTypes type = MetricsTypes.all;
  protected String prefix;
  protected String regex;
  protected String property;
  protected String key;

  /**
   * Base constructor pointing to endpoint of Metrics API.
   */
  public MetricsRequest() {
    super(METHOD.GET, "/admin/metrics");
  }

  /**
   *
   * @return
   */
  @Override
  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(METRICS_PARAM_GROUP, group.toString());
    params.set(METRICS_PARAM_TYPE, type.toString());
    params.set(METRICS_PARAM_PREFIX, prefix);
    params.set(METRICS_PARAM_REGEX, regex);
    params.set(METRICS_PARAM_PROPERTY, property);
    params.set(METRICS_PARAM_KEY, key);

    return params;
  }

  public static class NodeMetricsRequest extends MetricsRequest<MetricsResponse.NodeMetricsResponse> {
    public NodeMetricsRequest() {
      this.group = MetricsGroups.node;
    }

    @Override
    protected MetricsResponse.NodeMetricsResponse createResponse(SolrClient client) {
      return new MetricsResponse.NodeMetricsResponse();
    }
  }

  public static class CoreMetricsRequest extends MetricsRequest<MetricsResponse.CoreMetricsResponse> {
    public CoreMetricsRequest() {
      this.group = MetricsGroups.core;
    }

    @Override
    protected MetricsResponse.CoreMetricsResponse createResponse(SolrClient client) {
      return new MetricsResponse.CoreMetricsResponse();
    }
  }
}
