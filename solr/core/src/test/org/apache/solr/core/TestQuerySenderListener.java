/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.EventParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQuerySenderListener extends SolrTestCaseJ4 {

  // number of instances configured in the solrconfig.xml
  private static final int EXPECTED_MOCK_LISTENER_INSTANCES = 4;

  private static int preInitMockListenerCount = 0;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // record current value prior to core initialization,
    // so we can verify the correct number of instances later
    // NOTE: this won't work properly if concurrent tests run
    // in the same VM
    preInitMockListenerCount = MockEventListener.getCreateCount();

    initCore("solrconfig-querysender.xml", "schema.xml");
  }

  public void testListenerCreationCounts() {
    h.getCore();

    assertEquals(
        "Unexpected number of listeners created",
        EXPECTED_MOCK_LISTENER_INSTANCES,
        MockEventListener.getCreateCount() - preInitMockListenerCount);
  }

  @Test
  public void testRequestHandlerRegistry() {
    // property values defined in build.xml
    SolrCore core = h.getCore();

    assertEquals(2, core.firstSearcherListeners.size());
    assertEquals(2, core.newSearcherListeners.size());
  }

  @Test
  public void testSearcherEvents() throws Exception {
    SolrCore core = h.getCore();
    SolrEventListener newSearcherListener = core.newSearcherListeners.get(0);
    assertTrue(
        "Not an instance of QuerySenderListener",
        newSearcherListener instanceof QuerySenderListener);
    QuerySenderListener qsl = (QuerySenderListener) newSearcherListener;

    h.getCore()
        .withSearcher(
            currentSearcher -> {
              qsl.newSearcher(currentSearcher, null); // test new Searcher

              MockQuerySenderListenerReqHandler mock =
                  (MockQuerySenderListenerReqHandler) core.getRequestHandler("/mock");
              assertNotNull("Mock is null", mock);

              {
                String evt = mock.req.getParams().get(EventParams.EVENT);
                assertNotNull("Event is null", evt);
                assertEquals(
                    evt + " is not equal to " + EventParams.FIRST_SEARCHER,
                    EventParams.FIRST_SEARCHER,
                    evt);

                assertU(adoc("id", "1"));
                assertU(commit());
              }

              h.getCore()
                  .withSearcher(
                      newSearcher -> {
                        String evt = mock.req.getParams().get(EventParams.EVENT);
                        assertNotNull("Event is null", evt);
                        assertEquals(
                            evt + " is not equal to " + EventParams.NEW_SEARCHER,
                            EventParams.NEW_SEARCHER,
                            evt);
                        return null;
                      });

              return null;
            });
  }

  @Test
  public void testOtherCoreWarmingAfterRegistration() throws Exception {
    CoreContainer container = h.getCoreContainer();
    SolrCore source = h.getCore();
    SolrCore target = container.create("warmingTarget", Map.of("configSet", "minimal"));
    AtomicLong matches = new AtomicLong(-1);
    RequestHandlerBase recordingHandler =
        new RequestHandlerBase() {
          @Override
          public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
              throws Exception {
            target.getRequestHandler("/select").handleRequest(req, rsp);
            if (rsp.getException() != null) throw rsp.getException();
            matches.set(((ResultContext) rsp.getValues().get("response")).getDocList().matches());
          }

          @Override
          public String getDescription() {
            return "Records warming results";
          }

          @Override
          public Name getPermissionName(AuthorizationContext request) {
            return Name.READ_PERM;
          }
        };
    target.registerRequestHandler("/record-warming", recordingHandler);
    OtherCoreQuerySenderListener listener = new OtherCoreQuerySenderListener(source);
    NamedList<Object> query = new NamedList<>();
    query.add("qt", "/record-warming");
    query.add("q", "{!join from=id to=id fromIndex=" + source.getName() + "}id:91001");
    NamedList<Object> args = new NamedList<>();
    args.add("targetCore", "warmingTarget");
    args.add("queries", new ArrayList<>(List.of(query)));
    listener.init(args);
    source.postNewSearcherListeners.add(listener);
    try (EmbeddedSolrServer server = new EmbeddedSolrServer(container, target.getName())) {
      server.add(new SolrInputDocument("id", "91001"));
      server.commit();
      assertU(adoc("id", "91001"));
      assertU(commit());
      assertEquals("warming must see the newly registered source searcher", 1, matches.get());
      matches.set(-1);
      assertU(commit());
      assertEquals("an unchanged searcher must not trigger warming", -1, matches.get());
      assertU(delI("91001"));
      assertU(commit("softCommit", "true"));
      assertEquals("soft commits must warm against the new searcher", 0, matches.get());
    } finally {
      source.postNewSearcherListeners.remove(listener);
      container.unload("warmingTarget");
    }
  }

  @Test
  public void testOtherCoreListenerRejectsInvalidConfiguration() {
    OtherCoreQuerySenderListener listener = new OtherCoreQuerySenderListener(h.getCore());
    NamedList<Object> args = new NamedList<>();
    args.add("targetCore", "anotherCore");
    PluginInfo wrongEvent =
        new PluginInfo(
            "listener",
            Map.of("event", "newSearcher", "class", OtherCoreQuerySenderListener.class.getName()),
            args,
            List.of());
    assertThrows(SolrException.class, () -> listener.init(wrongEvent));

    args.add("sameShard", true);
    assertThrows(SolrException.class, () -> listener.init(args));

    args.remove("sameShard");
    NamedList<Object> query = new NamedList<>();
    query.add("q", "*:*");
    query.add("distrib", true);
    args.add("queries", new ArrayList<>(List.of(query)));
    assertThrows(SolrException.class, () -> listener.init(args));
  }
}
