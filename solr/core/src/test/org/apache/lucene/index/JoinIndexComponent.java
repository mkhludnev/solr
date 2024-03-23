package org.apache.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsReader;
import org.apache.lucene.codecs.lucene90.Lucene90PointsWriter;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PointValuesWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.lucene.index.TestPointsJoinIndex.dumpVisitor;

public class JoinIndexComponent extends SearchComponent implements SolrCoreAware {

    final private String indexDir = "joinindexdir";
    private FSDirectory index;

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {

    }

    public void writeJoinIndex() throws IOException {

        Codec codec = new Lucene95Codec();//getCodec();

        SegmentInfo segmentInfo =
                new SegmentInfo(
                        index,
                        Version.LATEST,
                        Version.LATEST,
                        "_0",
                        1,
                        false,
                        codec,
                        Collections.emptyMap(),
                        StringHelper.randomId(),
                        Collections.emptyMap(),
                        null);
        //FieldInfo proto = oneDocReader.getFieldInfos().fieldInfo("field");
        FieldInfo field =
                new FieldInfo(
                        "stub",
                        0,
                        false,
                        false,
                        false,
                        IndexOptions.NONE,
                        DocValuesType.NONE,
                        -1,
                        new HashMap<>(),
                        2,
                        2,
                        4,
                        0,
                        VectorEncoding.BYTE,
                        VectorSimilarityFunction.COSINE,
                        false);

        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] {field});

        IOContext ioContext = new IOContext(new FlushInfo(1, 20));
        SegmentWriteState writeState =
                new SegmentWriteState(
                        null, index, segmentInfo, fieldInfos, null, ioContext);
        try (PointsWriter writer = codec.pointsFormat().fieldsWriter(writeState)) {
            PointValuesWriter pointValuesWriter = new PointValuesWriter(Counter.newCounter(), field);
            BytesRef bytesRef = new BytesRef(new byte[Integer.BYTES * 2]);
            for(int i=0;i<10;i++) {
                IntPoint.encodeDimension(i, bytesRef.bytes, 0);
                IntPoint.encodeDimension(10-i, bytesRef.bytes, 4);
                pointValuesWriter.addPackedValue(0, bytesRef);
            }
            pointValuesWriter.flush(writeState, null, writer);
            writer.finish();
        }
        codec.fieldInfosFormat().write(index,segmentInfo,"",fieldInfos,ioContext);
        SegmentInfoFormat segmentInfoFormat = codec.segmentInfoFormat();
        segmentInfoFormat.write(index,segmentInfo, ioContext);
    }

    public void readJoinIndex() {


        SegmentInfo segmentInfo = null;
        FieldInfos fieldInfos = null;
        SegmentReadState readerState = new SegmentReadState(index, segmentInfo,fieldInfos, IOContext.READ );
        try(Lucene90PointsReader reader = new Lucene90PointsReader(readerState)){
            PointValues pointValues = reader.getValues("stub");//.intersect();
            pointValues.intersect(dumpVisitor(0,0,Integer.MAX_VALUE,5));
// PointValues values = reader.getPointValues(field);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDescription() {
        return "join index holder";
    }

    @Override
    public void inform(SolrCore core) {
        String dirPath;
        if (!new File(indexDir).isAbsolute()) {
            dirPath = core.getDataDir() + File.separator + indexDir;
        } else {
            dirPath = indexDir;
        }
        try {
            index = //new FilterDirectory(
                    FSDirectory.open(Path.of(dirPath));
            core.addCloseHook(new CloseHook() {
                @Override
                public void postClose(SolrCore core) {
                    try {
                        index.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
