package org.apache.lucene.index;

import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.search.ArrayJoinIndex;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.IOFunction;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.IOException;
import java.util.List;

public class JoinIndexComp2 extends SearchComponent implements SolrCoreAware {
    private SolrCore core;

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {

    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {

    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public void inform(SolrCore core) {
        this.core = core;
    }

    public void indexJoin(String core, String[] fields, String core2, String[] fields2) throws IOException {
        SolrCore solrCore2 = this.core.getCoreContainer().getCore(core2);
        Document mule = new Document();
        this.core.getCoreContainer().getCore(core).withSearcher(new IOFunction<SolrIndexSearcher, Void>() {
            @Override
            public Void apply(SolrIndexSearcher searcher1) throws IOException {
                solrCore2.withSearcher(new IOFunction<SolrIndexSearcher, Void>() {
                    @Override
                    public Void apply(SolrIndexSearcher search2) throws IOException {
                        for (LeafReaderContext lrc : searcher1.getTopReaderContext().leaves()) {
                            Object id1 = lrc.id();
                            for(String f1:fields) {
                                for (LeafReaderContext lrc2 : search2.getTopReaderContext().leaves()) {
                                    Object id2 = lrc2.id();
                                    for (String f2:fields2) {
                                        //IndexableFieldType fieldType = new FieldType();
                                        //byte[] arrOfBytes;
                                        byte[] docnum1;
                                        byte[] docnum2;
                                        String relationFieldName = id1.hashCode() + f1 + id2.hashCode() + id2;
                                        List<Integer>[] scratch = new ArrayJoinIndex.Intersector(f1, lrc, f2, lrc2).intersect();
                                        if (scratch!=null) {
                                            for (int i = 0; i < scratch.length; i++) {
                                                if (scratch[i] != null) {
                                                    for (int toDocNum : scratch[i]) { // notnull

                                                        mule.add(
                                                                new IntPoint(relationFieldName, i, toDocNum));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return null;
                    }
                });
                return null;
            }
        });
        //write(mule);
    }
}
