package org.apache.solr.search;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public abstract class ArrayJoinIndex implements JoinIndex {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static class Intersector {
        final private String fromField;
        final private LeafReaderContext fromCtx;
        final private String toField;
        final private LeafReaderContext toCtx;
        private PostingsEnum toPostings;
        private PostingsEnum fromPostings;
        private BytesRef fromTerm;

        public Intersector(String fromField, LeafReaderContext fromCtx, String toField, LeafReaderContext toCtx) {
            this.fromField = fromField;
            this.fromCtx = fromCtx;
            this.toField = toField;
            this.toCtx = toCtx;
        }

        /**
         * @return array of lists of out-of-order "to" docnums with possible dupes, by "from" doc number.
         * Or null if there are no intersection.
         *
         * TODO return sublists of huge array. is it worth it?
         * */
        public List<Integer>[] intersect() throws IOException {
            if (log.isTraceEnabled()) {
                log.trace("{} -> {}", fromField, fromCtx);
                log.trace("-> {}  {}", toField, toCtx);
            }
            List<Integer>[] toByFromScratch = null;
            final Terms fromTerms = fromCtx.reader().terms(fromField);
            final Terms toTerms = toCtx.reader().terms(toField);
            if (fromTerms != null && toTerms != null) {
                final TermsEnum fromIter = fromTerms.iterator();
                final TermsEnum toIter = toTerms.iterator();
                BytesRef targetTerm = null;
                EOF:
                for (; true; ) {
                    TermsEnum.SeekStatus fromSeek;
                    if (targetTerm == null) {
                        fromTerm = fromIter.next();
                        if (fromTerm == null) {
                            break;
                        }
                        fromSeek = TermsEnum.SeekStatus.NOT_FOUND;// we need to drag to counterpart
                    } else {
                        fromSeek = fromIter.seekCeil(targetTerm);
                        if (fromSeek == TermsEnum.SeekStatus.END) {
                            break;
                        }
                        fromTerm = fromIter.term();
                    }
                    if (fromSeek == TermsEnum.SeekStatus.NOT_FOUND) {
                        TermsEnum.SeekStatus seekStatus = toIter.seekCeil(fromTerm);
                        switch (seekStatus) {
                            case END:
                                break EOF;
                            case NOT_FOUND:
                                targetTerm = toIter.term();
                                continue;
                        }
                    }
                    assert fromTerm.bytesEquals(toIter.term()) : fromTerm.utf8ToString() + "!=" + toIter.term().utf8ToString();
                    toByFromScratch = onIntersection(fromIter, toIter, toByFromScratch);
                    targetTerm = null; // gimme next term
                }
            }
            return toByFromScratch;
        }

        @SuppressWarnings("unchecked")
        protected List<Integer>[] onIntersection(TermsEnum fromIter, TermsEnum toIter, List<Integer>[] toByFromScratch) throws IOException {
            int toDoc;
            int[] toDocs = new int[10]; // most times it should be one, I suppose ?? TODO reuse ?? use previous size as estimate?
            int toDocNum = 0;
            for (toPostings = toIter.postings(toPostings, PostingsEnum.NONE);
                 (toDoc = toPostings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS
                    ; ) {
                if (toCtx.reader().getLiveDocs() != null && !toCtx.reader().getLiveDocs().get(toDoc)) {
                    continue;
                }
                if (toDocNum >= toDocs.length) {
                    toDocs = ArrayUtil.grow(toDocs);
                }
                toDocs[toDocNum++] = toDoc;
            }
            if (toDocNum > 0) {
                int fromDoc;
                List<Integer> fromDocsTrace = null;
                if (log.isTraceEnabled()) {
                    fromDocsTrace = new ArrayList<>();
                }
                for (fromPostings = fromIter.postings(fromPostings, PostingsEnum.NONE);
                     (fromDoc = fromPostings.nextDoc()) != NO_MORE_DOCS
                        ;
                ) {
                    if (fromCtx.reader().getLiveDocs() != null && !fromCtx.reader().getLiveDocs().get(fromDoc)) {
                        continue;
                    }
                    if (toByFromScratch == null) {
                        toByFromScratch = (List<Integer>[]) Array.newInstance(List.class, fromCtx.reader().maxDoc());
                    }
                    if (toByFromScratch[fromDoc] == null) {
                        toByFromScratch[fromDoc] = new ArrayList<>();
                    }
                    for (int td = 0; td < toDocNum; td++) {
                        toByFromScratch[fromDoc].add(toDocs[td]);
                    }
                    if (log.isTraceEnabled()) {
                        fromDocsTrace.add(fromDoc + fromCtx.docBase);
                    }
                }
                if (log.isTraceEnabled()) {
                    final int[] docNums = toDocs;
                    log.trace("{}:{}=>{}", fromTerm.utf8ToString(), fromDocsTrace,
                            Arrays.toString(IntStream.range(0, toDocNum).map(i -> docNums[i] + toCtx.docBase).toArray()));
                }
            }
            return toByFromScratch;
        }
    }

    @SuppressWarnings("unchecked")
    public ArrayJoinIndex(String fromField, LeafReaderContext fromCtx, String toField, LeafReaderContext toCtx) throws IOException {
        List<Integer>[] scratch = new Intersector(fromField, fromCtx, toField, toCtx).intersect();

        if (scratch != null) {
            int[][] toByFrom2 = new int[scratch.length][];//TODO well, max toDoc is a little bit smaller
            int fromDocNum = 0;
            for (List<Integer> docNums : scratch) {
                if (docNums != null) {
                    toByFrom2[fromDocNum] = docNums.stream().distinct().sorted().mapToInt(Integer::intValue).toArray();
                    if (log.isTraceEnabled()) {
                        log.trace("{} -> {}", fromCtx.docBase + fromDocNum, Arrays.toString(docNums.stream().map(d -> d + toCtx.docBase).toArray()));
                    }
                }
                fromDocNum++;
            }
            toByFrom = toByFrom2;
        } else {
            toByFrom = null;
        }
    }

    //private
    private final int[][] toByFrom;

    @Override
    public boolean orIntersect(DocIdSetIterator fromDocs, Bits fromLives, Bits toLives, IntPredicate buff) throws IOException {
        int from;
        boolean hit = false;
        while ((from = fromDocs.nextDoc()) != NO_MORE_DOCS) {
            if (fromLives != null) {
                if (!fromLives.get(from)) {
                    continue;
                }
            }
            int[] toDocNums = toByFrom[from];
            if (toDocNums != null) {
                for (int to : toDocNums) {
                    if (toLives != null) {
                        if (!toLives.get(to)) {
                            continue;
                        }
                    }
                    hit |= buff.test(to);
                }
            }
        }
        return hit;
    }

    @Override
    public boolean isEmpty() {
        return toByFrom == null;
    }
}
