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
package org.apache.solr;

import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.join.TestScoreJoinQPNoScore;
import org.apache.solr.servlet.DirectSolrConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestCrossCoreJoin extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static SolrCore fromCore;

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.filterCache.async", "true");
    //    initCore("solrconfig.xml","schema12.xml");

    // File testHome = createTempDir().toFile();
    // FileUtils.copyDirectory(getFile("solrj/solr"), testHome);
    initCore("solrconfig.xml", "schema12.xml", TEST_HOME(), "collection1");
    final CoreContainer coreContainer = h.getCoreContainer();

    fromCore = coreContainer.create("fromCore", Map.of("configSet", "minimal",
            "schema", "dyn_fields_nocommit_schema.xml"));

    /* nocommit
    assertU(
        add(
            doc(
                "id",
                "1",
                "id_s_dv",
                "1",
                "name",
                "john",
                "title",
                "Director",
                "dept_s",
                "Engineering")));
    assertU(
        add(doc("id", "2", "id_s_dv", "2", "name", "mark", "title", "VP", "dept_s", "Marketing")));
    assertU(
        add(doc("id", "3", "id_s_dv", "3", "name", "nancy", "title", "MTS", "dept_s", "Sales")));
    assertU(
        add(
            doc(
                "id",
                "4",
                "id_s_dv",
                "4",
                "name",
                "dave",
                "title",
                "MTS",
                "dept_s",
                "Support",
                "dept_s",
                "Engineering")));
    assertU(
        add(
            doc(
                "id",
                "5",
                "id_s_dv",
                "5",
                "name",
                "tina",
                "title",
                "VP",
                "dept_s",
                "Engineering")));
    assertU(commit());

    update(
        fromCore,
        add(
            doc(
                "id",
                "10",
                "id_s_dv",
                "10",
                "dept_id_s",
                "Engineering",
                "text",
                "These guys develop stuff",
                "cat",
                "dev")));
    update(
        fromCore,
        add(
            doc(
                "id",
                "11",
                "id_s_dv",
                "11",
                "dept_id_s",
                "Marketing",
                "text",
                "These guys make you look good")));
    update(
        fromCore,
        add(
            doc(
                "id",
                "12",
                "id_s_dv",
                "12",
                "dept_id_s",
                "Sales",
                "text",
                "These guys sell stuff")));
    update(
        fromCore,
        add(
            doc(
                "id",
                "13",
                "id_s_dv",
                "13",
                "dept_id_s",
                "Support",
                "text",
                "These guys help customers")));
    update(fromCore, commit());*/
  }

  public static String update(SolrCore core, String xml, SolrParams params) throws Exception {
    DirectSolrConnection connection = new DirectSolrConnection(core);
    SolrRequestHandler handler = core.getRequestHandler("/update");
    return connection.request(handler, params, xml);
  }

  @Test
  public void testJoin() throws Exception {
    doTestJoin("{!join");
  }

  @Test
  public void testJoinIndex() throws Exception {
    doTestJoin("{!join method=joinIndex ");
  }

  @Test
  public void testScoreJoin() throws Exception {
    doTestJoin("{!join " + TestScoreJoinQPNoScore.whateverScore());
  }

  void doTestJoin(String joinPrefix) throws Exception {
    assertJQ(
        req(
            "q",
            joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev",
            "fl",
            "id",
            "debugQuery",
            random().nextBoolean() ? "true" : "false"),
        "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}");

    assertJQ(
        req(
            "qt",
            "/export",
            "q",
            joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev",
            "fl",
            "id_s_dv",
            "sort",
            "id_s_dv asc",
            "debugQuery",
            random().nextBoolean() ? "true" : "false"),
        "/response=={'numFound':3,'docs':[{'id_s_dv':'1'},{'id_s_dv':'4'},{'id_s_dv':'5'}]}");
    assertFalse(fromCore.isClosed());
    assertFalse(h.getCore().isClosed());

    // find people that develop stuff - but limit via filter query to a name of "john",
    // this tests filters being pushed down to queries (SOLR-3062)
    assertJQ(
        req(
            "q",
            joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev",
            "fl",
            "id",
            "fq",
            "name:john",
            "debugQuery",
            random().nextBoolean() ? "true" : "false"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'}]}");
  }

  @Test
  public void testCoresAreDifferent() throws Exception {
    assertQEx("schema12.xml" + " has no \"cat\" field", req("cat:*"), ErrorCode.BAD_REQUEST);
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(fromCore, "cat:*", "/select", 0, 100, Collections.emptyMap());
    final String resp = query(fromCore, req);
    assertTrue(resp, resp.contains("numFound=\"1\""));
    assertTrue(resp, resp.contains("<str name=\"id\">10</str>"));
  }


  public String query(SolrCore core, SolrQueryRequest req) throws Exception {
    return query(core, req, info -> {
      Writer sw = new StringWriter(32000);
      QueryResponseWriter responseWriter = info.getReq().getCore().getQueryResponseWriter(info.getReq());
      try {
        responseWriter.write(sw, info.getReq(), info.getRsp());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return sw.toString();
    });
  }

  public String query(SolrCore core, SolrQueryRequest req, Function<SolrRequestInfo, String> function) throws Exception {
    try {
      String handler = "standard";
      if (req.getParams().get("qt") != null) {
        handler = req.getParams().get("qt");
      }
      if (req.getParams().get("wt") == null) {
        ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
        params.set("wt", "xml");
        req.setParams(params);
      }
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo info = new SolrRequestInfo(req, rsp);
      SolrRequestInfo.setRequestInfo(info);
      try {
        core.execute(core.getRequestHandler(handler), req, rsp);
        if (rsp.getException() != null) {
          throw rsp.getException();
        }

        return  function.apply(info);
      }finally {
        SolrRequestInfo.clearRequestInfo();
      }
    }finally{
      req.close();
    }
  }

  @AfterClass
  public static void nukeAll() {
    fromCore = null;
  }


  @Test
  @SuppressWarnings({"unchecked","rawtypes"})
  public void testRandomJoin() throws Exception {
    int indexIter = 50 * RANDOM_MULTIPLIER;
    int queryIter = 50 * RANDOM_MULTIPLIER;

    // groups of fields that have any chance of matching... used to
    // increase test effectiveness by avoiding 0 resultsets much of the time.
    String[][] compat =
            new String[][] {
                    {"small_s", "small2_s", "small2_ss", "small3_ss"},
                //    {"small_i", "small2_i", "small2_is", "small3_is", "small_i_dv", "small_is_dv"}
            };

    while (--indexIter >= 0) {
      int indexSize = random().nextInt(20 * RANDOM_MULTIPLIER);

      List<FldType> types = new ArrayList<>();
      types.add(new FldType("id", ONE_ONE, new SVal('A', 'Z', 4, 4)));
      types.add(new FldType("score_f", ONE_ONE, new FVal(1, 100))); // field used to score
      types.add(
              new FldType("small_s", ZERO_ONE, new SVal('a', (char) ('c' + indexSize / 3), 1, 1)));
      types.add(
              new FldType("small2_s", ZERO_ONE, new SVal('a', (char) ('c' + indexSize / 3), 1, 1)));
      types.add(
              new FldType("small2_ss", ZERO_TWO, new SVal('a', (char) ('c' + indexSize / 3), 1, 1)));
      types.add(new FldType("small3_ss", new IRange(0, 25), new SVal('A', 'z', 1, 1)));
      /*types.add(new FldType("small_i", ZERO_ONE, new IRange(0, 5 + indexSize / 3)));
      types.add(new FldType("small2_i", ZERO_ONE, new IRange(0, 5 + indexSize / 3)));
      types.add(new FldType("small2_is", ZERO_TWO, new IRange(0, 5 + indexSize / 3)));
      types.add(new FldType("small3_is", new IRange(0, 25), new IRange(0, 100)));
      types.add(new FldType("small_i_dv", ZERO_ONE, new IRange(0, 5 + indexSize / 3)));
      types.add(new FldType("small_is_dv", ZERO_ONE, new IRange(0, 5 + indexSize / 3)));
*/
      clearIndex();
      update(fromCore, delQ("*:*"), null);
      @SuppressWarnings({"rawtypes"})
      Map<Comparable, Doc> model = indexDocs(types, null, indexSize);
      Map<Comparable, Doc> modelFromClone = indexClonesToFromCore(Collections.unmodifiableMap(model));
      @SuppressWarnings({"rawtypes"})
      Map<String, Map<Comparable, Set<Comparable>>> pivots = new HashMap<>();

      for (int qiter = 0; qiter < queryIter; qiter++) {
        String fromField;
        String toField;
        /* disable matching incompatible fields since 7.0... it doesn't work with point fields and doesn't really make sense?
        if (random().nextInt(100) < 5) {
          // pick random fields 5% of the time
          fromField = types.get(random().nextInt(types.size())).fname;
          // pick the same field 50% of the time we pick a random field (since other fields won't match anything)
          toField = (random().nextInt(100) < 50) ? fromField : types.get(random().nextInt(types.size())).fname;
        } else
        */
        {
          // otherwise, pick compatible fields that have a chance of matching indexed tokens
          String[] group = compat[random().nextInt(compat.length)];
          fromField = group[random().nextInt(group.length)];
          toField = group[random().nextInt(group.length)];
        }

        @SuppressWarnings({"rawtypes"})
        Map<Comparable, Set<Comparable>> pivot = pivots.get(fromField + "/" + toField);
        if (pivot == null) {
          pivot = createJoinMap(model, fromField, toField);
          pivots.put(fromField + "/" + toField, pivot);
        }

        Collection<Doc> fromDocs = model.values();
        List<Comparable> allIds = new ArrayList<>(model.keySet());
        Collections.shuffle(allIds, random());
        int splitPos = Math.min(atLeast(random(),1+allIds.size()/2), allIds.size());
        List<Comparable> matchingIds = allIds.subList(0, splitPos);

        List<Comparable> remainingIds = new ArrayList<>(allIds.subList(splitPos, allIds.size()));
        Collections.shuffle(remainingIds, random());
        remainingIds = remainingIds.isEmpty()?remainingIds: remainingIds.subList(0, random().nextInt(remainingIds.size()));
        splitPos = remainingIds.isEmpty()?0:random().nextInt(  remainingIds.size());
        List<Comparable> fromIdFilter = new ArrayList<>(matchingIds);
        List<Comparable> toIdFilter = new ArrayList<>(matchingIds);

        //fromIdFilter.addAll(remainingIds.subList(0, splitPos));
        fromIdFilter.add("PLACE_HOLDER");
       // toIdFilter.addAll(remainingIds.subList(splitPos, remainingIds.size()));
        toIdFilter.add("PLACE_HOLDER");
        List<Doc> fromSideFiltered = fromDocs.stream().filter(d -> matchingIds.contains(d.id)).collect(Collectors.toList());
        @SuppressWarnings({"rawtypes"})
        Set<Comparable> docs = join(fromSideFiltered, pivot);
        List<Comparable> toSideFiltered = docs.stream().filter(toId -> matchingIds.contains(toId)).collect(Collectors.toList());
        List<Doc> docList = new ArrayList<>(toSideFiltered.size());
        for (@SuppressWarnings({"rawtypes"}) Comparable id : toSideFiltered) docList.add(model.get(id));
        docList.sort(createComparator("_docid_", true, false, false, false));
        List<Object> sortedDocs = new ArrayList<>();
        for (Doc doc : docList) {
          if (sortedDocs.size() >= 10) break;
          sortedDocs.add(doc.toObject(h.getCore().getLatestSchema()));
        }

        Map<String, Object> resultSet = new LinkedHashMap<>();
        resultSet.put("numFound", docList.size());
        resultSet.put("start", 0);
        resultSet.put("numFoundExact", true);
        resultSet.put("docs", sortedDocs);

        // todo: use different join queries for better coverage

        SolrQueryRequest req =
                req(
                        "wt",
                        "json",
                        "indent",
                        "true",
                        "echoParams",
                        "all",
                        "q",
                        "{!join from="
                                + fromField
                                + " to="
                                + toField
                                + " fromIndex=fromCore"
                                //+ (random().nextInt(4) == 0 ? " fromIndex=collection1" : "")
                                + "}{!terms f=id}"+fromIdFilter.stream().map(Object::toString).collect(Collectors.joining(",")),
                        "fq","{!terms f=id}"+toIdFilter.stream().map(Object::toString).collect(Collectors.joining(",")));

        String strResponse = h.query(req);

        Object realResponse = Utils.fromJSONString(strResponse);
        String err = JSONTestUtil.matchObj("/response", realResponse, resultSet);
        if (err != null) {
          log.error(
                  "JOIN MISMATCH: {}\n\trequest={}\n\tresult={}\n\texpected={}\n\tmodel={}",
                  err,
                  req,
                  strResponse,
                  Utils.toJSONString(resultSet),
                  model);

          // re-execute the request... good for putting a breakpoint here for debugging
          String rsp = h.query(req);

          fail(err);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  private Map<Comparable, Doc> indexClonesToFromCore(Map<Comparable, Doc> model) throws Exception {

    // commit an average of 10 times for large sets, or 10% of the time for small sets
    int commitOneOutOf = Math.max(model.size() / 10, 10);

    HashMap<Comparable, Doc> clones = new HashMap<>();

    for (Map.Entry<Comparable, Doc> kv :
            model.entrySet()) {
      Doc clone = kv.getValue().shallowCopy();
      updateJ(fromCore,toJSON(clone), null);
      clones.put(clone.id, clone);
      // commit 10% of the time
      if (random().nextInt(commitOneOutOf) == 0) {
        update(fromCore, commit(), null);
      }
      // duplicate 10% of the docs
      if (random().nextInt(10) == 0) {
        updateJ(fromCore,toJSON(clone), null);
        clones.put(clone.id, clone);
      }
    }

    // optimize 10% of the time
    if (random().nextInt(10) == 0) {
      update(fromCore, commit(), new MapSolrParams(Map.of("optimize","true")));
    } else {
     // if (random().nextInt(10) == 0) {
      update(fromCore, commit(), null);
      /*} else {
        assertU(commit("softCommit", "true"));
      }*/
    }

    final LocalSolrQueryRequest req =
            new LocalSolrQueryRequest(fromCore, "*:*", "/select", 0, clones.size() * 2,
                    Map.of(   "fl",
                            "id",
                            "sort",
                            "_docid_ asc","wt",
                            "json",
                            "indent",
                            "true"));
    final String responseStr = query(fromCore, req, info -> {
      BinaryQueryResponseWriter responseWriter = (BinaryQueryResponseWriter) info.getReq().getCore().getQueryResponseWriter(info.getReq());
      ByteArrayOutputStream out;
      try {
        out = new ByteArrayOutputStream(3200);
        responseWriter.write(out, info.getReq(), info.getRsp());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return out.toString();
    });

    Object response = ObjectBuilder.fromJSON(responseStr);
    response = ((Map) response).get("response");
    response = ((Map) response).get("docs");
    @SuppressWarnings({"unchecked"})
    List<Map> docList = (List<Map>) response;
    int order = 0;
    //WHY? we won't query them anyway
    for (Map doc : docList) {
      Object id = doc.get("id");
      Doc modelDoc = clones.get(id);
      if (modelDoc == null) continue; // may be some docs in the index that aren't modeled
      modelDoc.order = order++;
    }

    // make sure we updated the order of all docs in the model
    assertEquals(order, model.size());
    assertEquals(order, clones.size());

    return clones;
  }

  @SuppressWarnings({"rawtypes"})
  Map<Comparable, Set<Comparable>> createJoinMap(
          Map<Comparable, Doc> model, String fromField, String toField) {
    Map<Comparable, Set<Comparable>> id_to_id = new HashMap<>();

    Map<Comparable, List<Comparable>> value_to_id = invertField(model, toField);

    for (Comparable fromId : model.keySet()) {
      Doc doc = model.get(fromId);
      List<Comparable> vals = doc.getValues(fromField);
      if (vals == null) continue;
      for (Comparable val : vals) {
        List<Comparable> toIds = value_to_id.get(val);
        if (toIds == null) continue;
        Set<Comparable> ids = id_to_id.get(fromId);
        if (ids == null) {
          ids = new HashSet<>();
          id_to_id.put(fromId, ids);
        }
        for (Comparable toId : toIds) ids.add(toId);
      }
    }

    return id_to_id;
  }

  @SuppressWarnings({"rawtypes"})
  Set<Comparable> join(Collection<Doc> input, Map<Comparable, Set<Comparable>> joinMap) {
    @SuppressWarnings({"rawtypes"})
    Set<Comparable> ids = new HashSet<>();
    for (Doc doc : input) {
      @SuppressWarnings({"rawtypes"})
      Collection<Comparable> output = joinMap.get(doc.id);
      if (output == null) continue;
      ids.addAll(output);
    }
    return ids;
  }

}
