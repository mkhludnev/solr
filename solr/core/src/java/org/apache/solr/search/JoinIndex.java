package org.apache.solr.search;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class JoinIndex {
    @SuppressWarnings("unchecked")
    public JoinIndex(String fromField, LeafReaderContext fromCtx, String toField, LeafReaderContext toCtx) throws IOException {
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
                (toDoc=toPostings.nextDoc())!= DocIdSetIterator.NO_MORE_DOCS &&
                        (toCtx.reader().getLiveDocs()==null || toCtx.reader().getLiveDocs().get(toDoc));) {
                if(toDocNum>=toDocs.length) {
                    toDocs=ArrayUtil.grow(toDocs);
                }
                toDocs[toDocNum++]=toDoc;
            }
            if(toDocNum>0) {
                //toDocs = ArrayUtil.copyOfSubArray(toDocs,0,toDocNum);
                // dump into toByFrom
                // allocate scratch
                int fromDoc;
                for(fromPostings = fromIter.postings(fromPostings, PostingsEnum.NONE);
                    (fromDoc=fromPostings.nextDoc())!= NO_MORE_DOCS &&
                            (fromCtx.reader().getLiveDocs()==null || fromCtx.reader().getLiveDocs().get(fromDoc));
                ){
                    if (toByFromScratch==null) {
                        toByFromScratch = (List<Integer>[]) Array.newInstance(List.class, fromCtx.reader().maxDoc());
                    }
                    if (toByFromScratch[fromDoc]==null){
                        toByFromScratch[fromDoc] = new ArrayList<>();
                    }
                    for (int td: toDocs) {
                        toByFromScratch[fromDoc].add(td);
                    }
                }
                // growScratch
            }
            targetTerm=null; // toTerm next
        }
        // remember min/max from/to for fast check
        if (toByFromScratch!=null){
            int[][] toByFrom2 = new int[toByFromScratch.length][];//TODO well, max toDoc is a little bit smaller
            int fromDocNum=0;
           for(List<Integer> docNums:toByFromScratch) {
               if (docNums!=null){
                   int  pos=0;
                   toByFrom2[fromDocNum] = new int[docNums.size()];
                   for (int nums:docNums) {
                       toByFrom2[fromDocNum][pos++] = nums;
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
                    if (to >= firstDocBuffered && to < firstDocBuffered+toBuffer.length())
                    toBuffer.set(to - firstDocBuffered);
                    hit = true;
                }
            }
        }
        return hit;
    }

    public boolean isEmpty() {
        return toByFrom==null;
    }
}
