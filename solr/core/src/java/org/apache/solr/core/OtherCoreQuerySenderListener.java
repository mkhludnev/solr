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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Warms other local cores after this core's new searcher is registered. */
public class OtherCoreQuerySenderListener extends AbstractSolrEventListener
    implements PluginInfoInitialized {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String targetCore;
  private String targetCollection;
  private boolean sameShard;
  private List<NamedList<Object>> queries;

  public OtherCoreQuerySenderListener(SolrCore core) {
    super(core);
  }

  @Override
  public void init(PluginInfo info) {
    if (!"postNewSearcher".equals(info.attributes.get("event"))) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "OtherCoreQuerySenderListener requires the postNewSearcher event");
    }
    init(info.initArgs);
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    targetCore = (String) args.get("targetCore");
    targetCollection = (String) args.get("targetCollection");
    Object shardOption = args.get("sameShard");
    if (shardOption != null
        && !"true".equalsIgnoreCase(shardOption.toString())
        && !"false".equalsIgnoreCase(shardOption.toString())) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "sameShard must be true or false");
    }
    sameShard = Boolean.parseBoolean(String.valueOf(shardOption));
    boolean cloud = getCore().getCoreDescriptor().getCloudDescriptor() != null;
    if (cloud
        ? targetCollection == null || targetCollection.isBlank() || targetCore != null
        : targetCore == null || targetCore.isBlank() || targetCollection != null || sameShard) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Specify targetCore in standalone mode or targetCollection in SolrCloud mode");
    }
    @SuppressWarnings("unchecked")
    ArrayList<Object> queryArgs = (ArrayList<Object>) args.getAll("queries");
    queries = QuerySenderListener.convertQueriesToList(queryArgs);
    for (NamedList<Object> query : queries) {
      if (query.toSolrParams().getBool(CommonParams.DISTRIB, false)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Warming queries must use distrib=false");
      }
    }
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    CoreContainer container = getCore().getCoreContainer();
    List<String> names = targetCore != null ? List.of(targetCore) : container.getLoadedCoreNames();
    int targets = 0;
    for (String name : names) {
      if (targetCollection != null) {
        CoreDescriptor descriptor = container.getCoreDescriptor(name);
        CloudDescriptor cloud = descriptor == null ? null : descriptor.getCloudDescriptor();
        if (cloud == null
            || !targetCollection.equals(cloud.getCollectionName())
            || (sameShard
                && !cloud
                    .getShardId()
                    .equals(getCore().getCoreDescriptor().getCloudDescriptor().getShardId()))) {
          continue;
        }
      }
      try (SolrCore target = container.getCore(name)) {
        if (target == null) {
          log.warn("Cannot warm core {}: it is not available", name);
          continue;
        }
        RefCounted<SolrIndexSearcher> holder = target.getRegisteredSearcher();
        if (holder == null) {
          log.warn("Cannot warm core {}: it has no registered searcher", name);
          continue;
        }
        targets++;
        try {
          for (NamedList<Object> query : queries) {
            NamedList<Object> params = addEventParms(currentSearcher, query);
            if (params.get(CommonParams.DISTRIB) == null) {
              params.add(CommonParams.DISTRIB, false);
            }
            try {
              QuerySenderListener.executeQuery(holder.get(), params);
            } catch (Exception e) {
              log.warn("Failed to warm core {} with query {}", name, query, e);
            }
          }
        } finally {
          holder.decref();
        }
      } catch (Exception e) {
        log.warn("Failed to access core {} for warming", name, e);
      }
    }
    if (targetCollection != null && targets == 0) {
      log.warn("No available local cores to warm for collection {}", targetCollection);
    }
  }
}
