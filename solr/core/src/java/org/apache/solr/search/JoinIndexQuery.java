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

package org.apache.solr.search;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.FixedBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Objects;

/**
 */
public class JoinIndexQuery extends JoinQuery {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static int PREFETCH_TO_BITS = 1024;

  public JoinIndexQuery(String fromField, String toField, String coreName, Query subQuery) {
    super(fromField, toField, coreName, subQuery);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    if (!(searcher instanceof SolrIndexSearcher)) {
      log.debug(
          "Falling back to JoinQueryWeight because searcher [{}] is not the required SolrIndexSearcher",
          searcher);
      return super.createWeight(searcher, scoreMode, boost);
    }

    final SolrIndexSearcher solrSearcher = (SolrIndexSearcher) searcher;
    final JoinQueryWeight weight =
        new JoinQueryWeight(solrSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    final SolrIndexSearcher fromSearcher = weight.fromSearcher;
    final SolrIndexSearcher toSearcher = weight.toSearcher;

      // TODO get from and to fields, check
      // TODO track from scores as well
      DocSet fromDocSet = DocSetUtil.createDocSet(fromSearcher, q, null);// top level stuff, to weight

      if (fromDocSet.size()==0) {
        return new MatchNoDocsQuery().createWeight(searcher, scoreMode, boost);
      }
      return new ConstantScoreWeight(this, boost) {
        @SuppressWarnings("unchecked")
        @Override
        public Scorer scorer(LeafReaderContext toContext) throws IOException {

          DirectoryReader fromReader = fromSearcher.getRawReader(); // raw reader to avoid extra wrapping overhead
          JoinIndex [] fromIndicesByLeafOrd = new JoinIndex[fromReader.leaves().size()];
          boolean isEmpty = true;
          for (LeafReaderContext fromCtx : fromReader.leaves()) {
            assert fromReader.leaves().get(fromCtx.ord) == fromCtx;
            SolrCache<JoinIndexKey, JoinIndex> joinIndex = toSearcher.getCache("joinIndex");
            JoinIndex fromToTo ;
            JoinIndexKey key = new JoinIndexKey(fromField, fromCtx, toField, toContext);;
            boolean reverse;
            if (reverse = (fromField.compareTo(toField)>0)) {
              key = key.reverse();
            }
            fromToTo = joinIndex.computeIfAbsent(
                    key,
                    k -> new ParArrJoinIndex(k.fromField, k.fromCtx,
                            k.toField,k.toCtx));
                    //k -> new ArrayJoinIndex(fromField, fromCtx, toField, toContext));
            fromIndicesByLeafOrd[fromCtx.ord] = reverse ? fromToTo.reverse() : fromToTo;
            isEmpty &=fromIndicesByLeafOrd[fromCtx.ord].isEmpty();
          }

          if (isEmpty) {
            return null;
          }

          final DocIdSetIterator toApproximation = // it seems only live one are pushed through
                  DocIdSetIterator.all(toContext.reader().maxDoc());

          //toContext.reader().getLiveDocs()

          //TODO if (fromDocSet.size()==0) {}, perhaps not necessary
          //final int docBase = toContext.docBase;
          return new ConstantScoreScorer(
              this,
              this.score(),
              scoreMode,
              new TwoPhaseIterator(toApproximation) {
                FixedBitSet toBuffer;
                int firstDocBuffered = DocIdSetIterator.NO_MORE_DOCS;
                boolean hasToHitsBuffered = true;
                @Override
                public boolean matches() throws IOException {
                  int toCandidate = approximation.docID();
                  /*
                  for (LeafReaderContext fromCtx : fromReader.leaves()) {
                    assert fromReader.leaves().get(fromCtx.ord) == fromCtx;
                    JoinIndex joinIndex = fromIndicesByLeafOrd[fromCtx.ord];
                    if (!joinIndex.isEmpty()) {
                      DocIdSetIterator fromDocs = fromDocSet.iterator(fromCtx);
                      if (fromDocs != null) {
                        for(int fromDoc=-1; (fromDoc=fromDocs.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS;){
                          int[] toDocs = joinIndex.toByFrom[fromDoc];
                          for (int i=0;toDocs!=null && i<toDocs.length; i++) {
                            if (toDocs[i]==toCandidate){
                              return true;
                            }
                          }
                        }
                      }
                    }

                  }
                  return false;
                  /*/
                  boolean buffered = toBuffer != null && firstDocBuffered <= toCandidate &&  toCandidate-firstDocBuffered < PREFETCH_TO_BITS;
                  if (!buffered) {
                    if (toBuffer == null) {
                      toBuffer =new FixedBitSet(PREFETCH_TO_BITS);
                    } else {
                      toBuffer.clear();
                    }
                    firstDocBuffered = toCandidate;
                    hasToHitsBuffered = false;
                    for (LeafReaderContext fromCtx : fromReader.leaves()) {
                      assert fromReader.leaves().get(fromCtx.ord) == fromCtx;
                      JoinIndex joinIndex = fromIndicesByLeafOrd[fromCtx.ord];
                      if (!joinIndex.isEmpty()) {
                        DocIdSetIterator fromDocs = fromDocSet.iterator(fromCtx);
                        if (fromDocs != null) {
                          boolean hit = joinIndex.orIntersection(fromDocs,
                                  fromCtx.reader().getLiveDocs(),
                                  toContext.reader().getLiveDocs(),
                                  firstDocBuffered,
                                  toBuffer);
                          hasToHitsBuffered |= hit;
                        }
                      }
                    }
                  }
                  return hasToHitsBuffered && toBuffer.get(toCandidate-firstDocBuffered);
                  //*/
                }
                @Override
                public float matchCost() {
                  return fromDocSet.size();
                }
              });
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
  }

  private static class JoinIndexKey {
    final String fromField;
    final LeafReaderContext fromCtx;
    final String toField;
    final LeafReaderContext toCtx;

    public JoinIndexKey(String fromField, LeafReaderContext fromCtx, String toField, LeafReaderContext toCtx) {
      this.fromField = fromField;
      this.fromCtx = fromCtx;
      this.toField = toField;
      this.toCtx = toCtx;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JoinIndexKey that = (JoinIndexKey) o;
      return Objects.equals(fromField, that.fromField) && Objects.equals(fromCtx.id(), that.fromCtx.id()) &&
              Objects.equals(toField, that.toField) && Objects.equals(toCtx.id(), that.toCtx.id());
    }

    @Override
    public int hashCode() {
      return Objects.hash(fromField, fromCtx.id(), toField, toCtx.id());
    }

    public JoinIndexKey reverse() {
      return new JoinIndexKey(toField, toCtx, fromField, fromCtx);
    }
  }
}
