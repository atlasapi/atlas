package org.atlasapi.remotesite.lovefilm;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;

public class LoveFilmData {

    private final Splitter splitter = Splitter.on("\",\"")
            .trimResults(CharMatcher.is('"'));
    
    public static final class LoveFilmDataRow {

        private final List<String> rowValues;
        private final List<String> columns;

        public LoveFilmDataRow(List<String> rowValues, List<String> columns) {
            this.rowValues = rowValues;
            this.columns = columns;
        }
        
        public String getColumnValue(String columnName) {
            int columnIndex = columns.indexOf(columnName);
            if (columnIndex < 0) {
                throw new RuntimeException("Column not found: " + columnName);
            }
            return rowValues.get(columnIndex);
        }
        
        public boolean columnValueIs(String columnName, String expectedValue) {
            return expectedValue.equals(getColumnValue(columnName));
        }
        
        @Override
        public String toString() {
            return rowValues.toString();
        }
        
    }

    private InputSupplier<? extends Reader> dataSupplier;
    
    public LoveFilmData(InputSupplier<? extends Reader> dataSupplier) {
        this.dataSupplier = dataSupplier;
    }
    
    public <T> T processData(LoveFilmDataProcessor<T> processor) throws IOException {
        return CharStreams.readLines(dataSupplier, lineProcessorWrap(processor));
    }
    
    private <T> LineProcessor<T> lineProcessorWrap(final LoveFilmDataProcessor<T> processor) {
        return new LineProcessor<T>() {

            List<String> headers;
            
            @Override
            public boolean processLine(String line) throws IOException {
                if (headers == null) {
                    headers = readHeaders(line);
                    return true;
                }
                return processor.process(rowFrom(line));
            }

            private LoveFilmDataRow rowFrom(String line) {
                List<String> values = ImmutableList.copyOf(splitter.split(line));
                return new LoveFilmDataRow(values, headers);
            }

            private List<String> readHeaders(String line) {
                return ImmutableList.copyOf(splitter.split(line));
            }

            @Override
            public T getResult() {
                return processor.getResult();
            }
            
        };
        
    }
}
