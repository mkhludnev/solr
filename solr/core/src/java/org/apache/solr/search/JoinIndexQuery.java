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
/*
      final SortedSetDocValues topLevelFromDocValues =
          validateAndFetchDocValues(fromSearcher, fromField, "from");
      final SortedSetDocValues topLevelToDocValues =
          validateAndFetchDocValues(toSearcher, toField, "to");
      if (topLevelFromDocValues.getValueCount() == 0 || topLevelToDocValues.getValueCount() == 0)
  */
   /*   if (false){
        return createNoMatchesWeight(boost);
      }
*/
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
            JoinIndex fromToTo = joinIndex.computeIfAbsent(
                    new JoinIndexKey(fromField, fromCtx.id(), toField, toContext.id()),
                    k -> new JoinIndex(fromField, fromCtx, toField, toContext));
            fromIndicesByLeafOrd[fromCtx.ord] = fromToTo;
            isEmpty &=fromToTo.isEmpty();
          }

          if (isEmpty) {
            return null;
          }

          final DocIdSetIterator toApproximation = // it seems only live one are pushed through
                  DocIdSetIterator.all(toContext.reader().maxDoc());

          //toContext.reader().getLiveDocs()
          // TODO track from scores as well
          DocSet fromDocSet = DocSetUtil.createDocSet(fromSearcher, q, null);
          //TODO if (fromDocSet.size()==0) {}, perhaps not necessary
          final int docBase = toContext.docBase;
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
                  boolean buffered = toBuffer != null && firstDocBuffered <= toCandidate && firstDocBuffered + toCandidate < 1024;
                  if (!buffered) {
                    if (toBuffer == null) {
                      toBuffer =new FixedBitSet(1024);
                    } else {
                      toBuffer.clear();
                    }
                    firstDocBuffered = toCandidate;
                    hasToHitsBuffered = false;
                    for (LeafReaderContext fromCtx : fromReader.leaves()) {
                      assert fromReader.leaves().get(fromCtx.ord) == fromCtx;
                      LeafReader fromReader = fromCtx.reader();
                      DocIdSetIterator fromDocs = fromDocSet.iterator(fromCtx);
                      if (fromDocs!=null) {
                        boolean hit = fromIndicesByLeafOrd[fromCtx.ord].orIntersection(fromDocs, firstDocBuffered, toBuffer);
                        hasToHitsBuffered |= hit;
                      }
                    }
                  }
                  return hasToHitsBuffered && toBuffer.get(toCandidate+firstDocBuffered);
                }
                @Override
                public float matchCost() {
                  return 1024;
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
    final Object fromCtx;
    final String toField;
    final Object toCtx;

    public JoinIndexKey(String fromField, Object fromCtx, String toField, Object toCtx) {
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
      return Objects.equals(fromField, that.fromField) && Objects.equals(fromCtx, that.fromCtx) && Objects.equals(toField, that.toField) && Objects.equals(toCtx, that.toCtx);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fromField, fromCtx, toField, toCtx);
    }
  }
}
