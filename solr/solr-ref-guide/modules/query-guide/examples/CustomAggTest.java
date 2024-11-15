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

package org.apache.solr.client.ref_guide_examples;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.json.DomainMap;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.request.json.QueryFacetMap;
import org.apache.solr.client.solrj.request.json.RangeFacetMap;
import org.apache.solr.client.solrj.request.json.TermsFacetMap;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.json.BucketJsonFacet;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.facet.AggValueSource;
import org.apache.solr.search.facet.FacetMerger;
import org.apache.solr.search.facet.SimpleAggValueSource;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example SolrJ usage of Custom Aggregation in Facet API.
 */
public class CustomAggTest extends SolrCloudTestCase {
    private static final String COLLECTION_NAME = "techproducts";
    private static final String CONFIG_NAME = "techproducts_config";

    // tag::custom-agg-impl[]
    public static class CustomFacet extends ValueSourceParser {
        @Override
        public ValueSource parse(FunctionQParser fp) throws SyntaxError {
            String v = fp.getLocalParams().get("myfield");
            return new AggValueSource(fp.getString()) {
                @Override
                public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext) throws IOException {
                    return new FunctionValues() {
                        @Override
                        public String toString(int doc) throws IOException {
                            return "";
                        }
                    };
                }

                @Override
                public FacetMerger createFacetMerger(Object prototype) {
                    return null;
                }

                @Override
                public boolean equals(Object o) {
                    return false;
                }

                @Override
                public int hashCode() {
                    return 0;
                }

                @Override
                public String description() {
                    return "";
                }
            };
        }
    }
    // end::custom-agg-impl[]

    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(1)
                .addConfig(CONFIG_NAME, new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
                .configure();

        CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
                .process(cluster.getSolrClient());

        ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update");
        up.setParam("collection", COLLECTION_NAME);
        up.addFile(getFile("solrj/techproducts.xml"), "application/xml");
        up.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        UpdateResponse updateResponse = up.process(cluster.getSolrClient());
        assertEquals(0, updateResponse.getStatus());

        V2Request req = new V2Request.Builder("/c/" + COLLECTION_NAME + "/config")
                .withMethod(SolrRequest.METHOD.POST)
                .withPayload(Map.of("add-valuesourceparser", Map.of("name", "agg_custom",
                        "class", "org.apache.solr.client.ref_guide_examples.CustomAggTest$CustomFacet")))
                .build();

        NamedList<Object> res = cluster.getSolrClient().request(req);
        assertTrue("The request failed", res.get("responseHeader").toString().contains("status=0"));
    }

    @Test
    public void testCustomAgg() throws Exception {
        SolrClient solrClient = cluster.getSolrClient();
        final int expectedResults = 4;

        final ModifiableSolrParams params = new ModifiableSolrParams();
        // tag::custom-agg-api-req[]
        final SolrQuery query = new SolrQuery("*:*");
        query.setParam("json.facet.test_custom_func", Utils.toJSONString(Map.of("type", "func",
                "func", "custom", "myfield", "test")));
        QueryResponse queryResponse = solrClient.query(COLLECTION_NAME, query);
        // end::custom-agg-api-req[]
        System.out.println(queryResponse);
       // assertResponseFoundNumDocs(queryResponse, expectedResults);
    }
}