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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class JoinIndex {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("unchecked")
    public JoinIndex(String fromField, LeafReaderContext fromCtx, String toField, LeafReaderContext toCtx) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("{} -> {}",fromField,fromCtx);
            log.trace("-> {}  {}",toField, toCtx);
        }
        Terms fromTerms = fromCtx.reader().terms(fromField);
        Terms toTerms = toCtx.reader().terms(toField);
        if (fromTerms==null || toTerms==null) {
            this.toByFrom = null;
            return;
        }
        TermsEnum fromIter=fromTerms.iterator();
        TermsEnum toIter = toTerms.iterator();
        // TODO prefix for tries
        BytesRef targetTerm = null;

        PostingsEnum fromPostings = null;
        PostingsEnum toPostings = null;
        List<Integer>[] toByFromScratch = null;
        EOF:
        for(BytesRef fromTerm; true;) {
            TermsEnum.SeekStatus fromSeek;
            if (targetTerm==null) {
                fromTerm = fromIter.next();
                if (fromTerm==null) {
                    break ; //TODO over?what todo?
                }
                fromSeek = TermsEnum.SeekStatus.NOT_FOUND;// we need to drag to counterpart
            } else {
                fromSeek = fromIter.seekCeil(targetTerm);
                if(fromSeek== TermsEnum.SeekStatus.END) {
                    break;
                }
                fromTerm = fromIter.term();
            }
            if (fromSeek==TermsEnum.SeekStatus.NOT_FOUND) {
                TermsEnum.SeekStatus seekStatus = toIter.seekCeil(fromTerm);
                switch (seekStatus) {
                    case END:
                        break EOF;
                    case NOT_FOUND:
                        targetTerm = toIter.term();
                        continue;
                }
            }
            assert fromTerm.bytesEquals(toIter.term()): fromTerm.utf8ToString()+"!="+toIter.term().utf8ToString();

            int toDoc;
            int [] toDocs = new int[10]; // most times it should be one, I suppose ?? TODO reuse ?? use previous size as estimate?
            int toDocNum = 0 ;
            for(toPostings = toIter.postings(toPostings, PostingsEnum.NONE);
                (toDoc=toPostings.nextDoc())!= DocIdSetIterator.NO_MORE_DOCS
                        ;) {
                if (toCtx.reader().getLiveDocs()!=null && !toCtx.reader().getLiveDocs().get(toDoc)) {
                    continue ;
                }
                if(toDocNum>=toDocs.length) {
                    toDocs=ArrayUtil.grow(toDocs);
                }
                if (toDocNum==0 || (toDocs[toDocNum]!=toDoc && toDocs[0]!=toDoc)) {
                    toDocs[toDocNum++] = toDoc;
                }
            }
            if(toDocNum>0) {
                //toDocs = ArrayUtil.copyOfSubArray(toDocs,0,toDocNum);
                // dump into toByFrom
                // allocate scratch
                int fromDoc;
                List<Integer> fromDocsTrace=null;
                if (log.isTraceEnabled()) {
                    fromDocsTrace = new ArrayList<>();
                }
                for(fromPostings = fromIter.postings(fromPostings, PostingsEnum.NONE);
                    (fromDoc=fromPostings.nextDoc())!= NO_MORE_DOCS
                            ;
                ){
                    if (fromCtx.reader().getLiveDocs()!=null && !fromCtx.reader().getLiveDocs().get(fromDoc))
                    {
                        continue ;
                    }
                    if (toByFromScratch==null) {
                        toByFromScratch = (List<Integer>[]) Array.newInstance(List.class, fromCtx.reader().maxDoc());
                    }
                    if (toByFromScratch[fromDoc]==null){
                        toByFromScratch[fromDoc] = new ArrayList<>();
                    }
                    for (int td=0;td<toDocNum;td++) {
                        toByFromScratch[fromDoc].add(toDocs[td]);
                    }
                    if (log.isTraceEnabled()) {
                        fromDocsTrace.add( fromDoc + fromCtx.docBase);
                    }
                }
                if (log.isTraceEnabled()) {
                    final int [] docNums = toDocs;
                    log.trace("{}:{}=>{}", fromTerm.utf8ToString(), fromDocsTrace,
                            Arrays.toString(IntStream.range(0,toDocNum).map(i->docNums[i]+toCtx.docBase).toArray()));
                }
            }
            targetTerm=null; // toTerm next
        }
        // remember min/max from/to for fast check
        if (toByFromScratch!=null){
            int[][] toByFrom2 = new int[toByFromScratch.length][];//TODO well, max toDoc is a little bit smaller
            int fromDocNum=0;
           for(List<Integer> docNums:toByFromScratch) {
               if (docNums!=null){
                   LinkedHashSet<Integer> toDocsUniq = new LinkedHashSet<>(docNums);
                   toByFrom2[fromDocNum] = toDocsUniq.stream().mapToInt(Integer::intValue).toArray();
                   if (log.isTraceEnabled()) {
                       log.trace("{} -> {}", fromCtx.docBase+fromDocNum, Arrays.toString(docNums.stream().map(d->d+toCtx.docBase).toArray()));
                   }
               }
               fromDocNum++;
           }
           toByFrom = toByFrom2;
        }
        else {
            toByFrom = null;
        }
    }
    // TODO maybe think about sparse format?? due to segments it should worth to explore

    //private
    private final int[][] toByFrom;

    public boolean orIntersection(DocIdSetIterator fromDocs, Bits fromLives, Bits toLives, int firstDocBuffered, FixedBitSet toBuffer) throws IOException {
        if (isEmpty()) {
            return false;
        }
        int from;
        boolean hit = false;
        while ((from=fromDocs.nextDoc())!=NO_MORE_DOCS) {
            if (fromLives!=null) {
                if (!fromLives.get(from)) {
                    continue;
                }
            }
            int[] toDocNums = toByFrom[from];
            if ( toDocNums!=null ) {
                for (int to : toDocNums) {
                    if (toLives!=null) {
                        if(!toLives.get(to)) {
                            continue;
                        }
                    }
                    if (to >= firstDocBuffered && to < firstDocBuffered+toBuffer.length()) {
                        toBuffer.set(to - firstDocBuffered);
                        hit = true;
                    }
                }
            }
        }
        return hit;
    }

    public boolean isEmpty() {
        return toByFrom==null;
    }
}
