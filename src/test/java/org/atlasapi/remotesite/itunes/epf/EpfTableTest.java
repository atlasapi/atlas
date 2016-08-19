package org.atlasapi.remotesite.itunes.epf;

import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.INTEGER;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.STRING;
import static org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.remotesite.itunes.epf.model.EpfTableColumn;
import org.atlasapi.remotesite.itunes.epf.model.EpfTableRow;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.metabroadcast.common.time.Timestamp;

public class EpfTableTest extends TestCase {

    private final Joiner fsJoiner = Joiner.on((char)1);
    
    private static class TestEpfRow extends EpfTableRow {
        public TestEpfRow(List<String> rowParts) {
            super(rowParts);
        }
        private static int iota = 0;
        public static final EpfTableColumn<Timestamp> EXPORT_DATE = new EpfTableColumn<Timestamp>(iota++, TIMESTAMP){};
        public static final EpfTableColumn<Integer> ARTIST_ID = new EpfTableColumn<Integer>(iota++, INTEGER){};
        public static final EpfTableColumn<String> NAME = new EpfTableColumn<String>(iota++, STRING){};
    }
    
    private static class CapturingTableRowProcessor implements EpfTableRowProcessor<TestEpfRow, List<TestEpfRow>> {

        private final ImmutableList.Builder<TestEpfRow> results = ImmutableList.builder();
        
        @Override
        public boolean process(TestEpfRow row) {
            results.add(row);
            return true;
        }

        @Override
        public List<TestEpfRow> getResult() {
            return results.build();
        }
        
    }

    private final Function<List<String>, TestEpfRow> convertToTestRow = new Function<List<String>, TestEpfRow>() {
        @Override
        public TestEpfRow apply(List<String> input) {
            return new TestEpfRow(input);
        }
    };

    @Test
    public void testCallsProcessorForRows() throws IOException {

        final CapturingTableRowProcessor processor = new CapturingTableRowProcessor();
        
        final String timestamp = "1320832802897";
        final String id = "341126";
        final String name = "Zoltán Kocsis";
        
        final String rowString = fsJoiner.join(ImmutableList.of(timestamp,id,name+((char)2)+"\n"));
        
        EpfTable<TestEpfRow> table = new EpfTable<TestEpfRow>(CharStreams.newReaderSupplier(rowString), convertToTestRow);
        
        List<TestEpfRow> processed = table.processRows(processor);
        
        assertThat(processed.size(), is(1));
        
        TestEpfRow row = processed.get(0);
        assertThat(row.get(TestEpfRow.EXPORT_DATE), is(equalTo(Timestamp.of(Long.valueOf(timestamp)))));
        assertThat(row.get(TestEpfRow.ARTIST_ID), is(equalTo(Integer.valueOf(id))));
        assertThat(row.get(TestEpfRow.NAME), is(equalTo(name)));
        
    }

    @Test
    public void testTrimsRowsCorrectly() throws IOException {
        
        final CapturingTableRowProcessor processor = new CapturingTableRowProcessor();
        
        final String rowString = fsJoiner.join(ImmutableList.of("1321437625956","1609240",""+((char)2)+"\n"));
        
        EpfTable<TestEpfRow> table = new EpfTable<TestEpfRow>(CharStreams.newReaderSupplier(rowString), convertToTestRow);
        
        List<TestEpfRow> processed = table.processRows(processor);
        
        assertThat(processed.size(), is(1));
        
        TestEpfRow row = processed.get(0);
        assertThat(row.get(TestEpfRow.NAME), is(equalTo("")));
        
    }

    @Test
    public void testProcessesRowsOverMultipleLines() throws IOException {
        
        final CapturingTableRowProcessor processor = new CapturingTableRowProcessor();
        
        final String id = "341126";
        String name = "On a \nnew line";
        final String rowString = fsJoiner.join(ImmutableList.of("1321437625956",id+"\n","\n"+name+((char)2)+"\n"));
        
        EpfTable<TestEpfRow> table = new EpfTable<TestEpfRow>(CharStreams.newReaderSupplier(rowString), convertToTestRow);
        
        List<TestEpfRow> processed = table.processRows(processor);
        
        assertThat(processed.size(), is(1));
        
        TestEpfRow row = processed.get(0);
        assertThat(row.get(TestEpfRow.ARTIST_ID), is(equalTo(Integer.valueOf(id))));
        assertThat(row.get(TestEpfRow.NAME), is(equalTo(name)));
        
    }
}
