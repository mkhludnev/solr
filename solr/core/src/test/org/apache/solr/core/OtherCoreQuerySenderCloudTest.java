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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.cluster.placement.plugins.StubShardAffinityPlacementFactory;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

public class OtherCoreQuerySenderCloudTest extends SolrCloudTestCase {
  private static final String TARGET = "warmTarget";
  private static final String SOURCE = "affinitySource";
  private static final String SINGLE = "singleSource";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("sourceConfig", configset("cloud-minimal"))
        .addConfig("singleConfig", configset("cloud-minimal"))
        .configure();
    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = StubShardAffinityPlacementFactory.class.getName();
    plugin.config = new AffinityPlacementConfig(1, 2, Map.of(), Map.of(SOURCE, TARGET), Map.of());
    new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .POST()
        .withPayload(Map.of("add", plugin))
        .build()
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(TARGET, "_default", 2, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    CollectionAdminRequest.addReplicaToShard(TARGET, "shard2")
        .setNode(cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(SOURCE, "sourceConfig", 2, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(SINGLE, "singleConfig", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(TARGET, 2, 3);
    cluster.waitForActiveCollection(SOURCE, 2, 2);
    cluster.waitForActiveCollection(SINGLE, 1, 1);
    addListener(SOURCE, true);
    addListener(SINGLE, false);
  }

  @Test
  public void testAllLocalShardsAndAffinityShard() throws Exception {
    CoreContainer container = cluster.getJettySolrRunner(0).getCoreContainer();
    Set<String> visited = ConcurrentHashMap.newKeySet();
    Set<String> allLocal = ConcurrentHashMap.newKeySet();
    Set<String> sameShard = ConcurrentHashMap.newKeySet();
    for (Replica replica :
        cluster.getSolrClient().getClusterState().getCollection(TARGET).replicaStream().toList()) {
      if (replica.getNodeName().equals(cluster.getJettySolrRunner(0).getNodeName())) {
        allLocal.add(replica.getCoreName());
        if (replica.getShard().equals("shard1")) sameShard.add(replica.getCoreName());
      }
      try (SolrCore target =
          cluster.getReplicaJetty(replica).getCoreContainer().getCore(replica.getCoreName())) {
        addAndCommit(target, "single-document");
        addAndCommit(target, "affinity-document");
        target.registerRequestHandler(
            "/record-warming",
            new RequestHandlerBase() {
              @Override
              public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
                req.getCore().getRequestHandler("/select").handleRequest(req, rsp);
                if (rsp.getException() == null
                    && ((ResultContext) rsp.getValues().get("response")).getDocList().matches()
                        == 1) {
                  visited.add(req.getCore().getName());
                }
              }

              @Override
              public String getDescription() {
                return "Records warmed shards";
              }

              @Override
              public Name getPermissionName(AuthorizationContext request) {
                return Name.READ_PERM;
              }
            });
      }
    }

    Replica single =
        cluster
            .getSolrClient()
            .getClusterState()
            .getCollection(SINGLE)
            .getSlice("shard1")
            .getLeader();
    try (SolrCore source = container.getCore(single.getCoreName())) {
      addAndCommit(source, "single-document");
      assertEquals(2, allLocal.size());
      assertEquals(allLocal, visited);
    }

    visited.clear();
    Replica replica =
        cluster
            .getSolrClient()
            .getClusterState()
            .getCollection(SOURCE)
            .getSlice("shard1")
            .getLeader();
    try (SolrCore source = container.getCore(replica.getCoreName())) {
      addAndCommit(source, "affinity-document");
      assertEquals(1, sameShard.size());
      assertEquals(sameShard, visited);
    }
  }

  private static void addListener(String collection, boolean sameShard) throws Exception {
    String command =
        "{'add-listener': {'name':'warm-other', 'event':'postNewSearcher', "
            + "'class':'solr.OtherCoreQuerySenderListener', 'targetCollection':'"
            + TARGET
            + "', 'sameShard':"
            + sameShard
            + ", 'queries':[{'qt':'/record-warming', 'q':'"
            + "{!join from=id to=id checkRouterField=false fromIndex="
            + collection
            + "}*:*'}]}}";
    cluster.getSolrClient().request(new ConfigRequest(command), collection);
    Replica replica =
        cluster.getSolrClient().getClusterState().getCollection(collection).getLeader("shard1");
    CoreContainer container = cluster.getReplicaJetty(replica).getCoreContainer();
    new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "postNewSearcher listener to be installed",
            () -> {
              try (SolrCore core = container.getCore(replica.getCoreName())) {
                return core != null && !core.postNewSearcherListeners.isEmpty();
              }
            });
  }

  private static void addAndCommit(SolrCore core, String id) throws Exception {
    try (SolrQueryRequest req = new SolrQueryRequestBase(core, params())) {
      AddUpdateCommand add = new AddUpdateCommand(req);
      add.solrDoc = new SolrInputDocument("id", id);
      core.getUpdateHandler().addDoc(add);
      core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
    }
  }
}
