/*

 Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
 all rights reserved.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 Except as contained in this notice, the name of Stanford University shall not
 be used in advertising or otherwise to promote the sale, use or other dealings
 in this Software without prior written authorization from Stanford University.

 */
package org.lockss.laaws.rs.io.index.solr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.NamedList;
import org.lockss.log.L4JLogger;

/**
 * Solr HTTP client that provides optional authentication with each request.
 */
public class LockssHttpSolrClient extends HttpSolrClient {
  private static final long serialVersionUID = -913433609253654644L;
  private static L4JLogger log = L4JLogger.getLogger();

  @Nullable
  private final String user;
  @Nullable
  private final String password;

  /**
   * Constructor.
   *
   * @param solrCollectionUrl
   *          A String with the Solr collection URL.
   * @param user
   *          A String with the name of the user to be authenticated.
   * @param password
   *          A String with the password of the user to be authenticated.
   */
  LockssHttpSolrClient(String solrCollectionUrl, String user, String password) {
    super(solrCollectionUrl, null, new BinaryResponseParser(), false, null);
    this.user = user;
    this.password = password;
  }

  /**
   * Overrides HttpSolrClient method to provide authentication credentials with
   * a request.
   */
  @SuppressWarnings("rawtypes")
  @Override
  public NamedList<Object> request(SolrRequest request,
      ResponseParser processor, String collection)
	  throws SolrServerException, IOException {
    // Get the request to be executed.
    HttpRequestBase method = createMethod(request, collection);

    // Check whether authentication should be set up.
    if (user != null && password != null) {
      // Yes: Set up the request authentication header.
      method.setHeader(new BasicHeader("Authorization", "Basic "
	  + Base64.byteArrayToBase64((user + ":" + password).
	      getBytes(StandardCharsets.UTF_8.name()))));
      log.debug("Authentication credentials user = {}", user);
    } else {
      log.debug("No authentication credentials");
    }

    // Make the request and return the result.
    return executeMethod(method, processor);
  }
}
