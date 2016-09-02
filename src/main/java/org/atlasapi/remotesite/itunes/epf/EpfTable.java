package org.atlasapi.remotesite.itunes.epf;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.atlasapi.remotesite.itunes.epf.model.EpfTableRow;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;

public class EpfTable<ROW extends EpfTableRow> {

    private static final Joiner EMPTY_JOINER = Joiner.on("");
    private final Splitter splitter;
    private final InputSupplier<? extends Reader> inputSupplier;
    private final Function<List<String>, ROW> splitLineExtractor;
    private final String rowSeparator;
    
    public EpfTable(InputSupplier<? extends Reader> inputSupplier, Function<List<String>, ROW> splitLineExtractor) {
        this(inputSupplier, splitLineExtractor, String.valueOf((char) 1), String.valueOf((char)2));
    }
    
    public EpfTable(InputSupplier<? extends Reader> inputSupplier, Function<List<String>, ROW> splitLineExtractor, String fieldSeparator, String rowSeparator) {
        this.inputSupplier = inputSupplier;
        this.splitLineExtractor = splitLineExtractor;
        this.rowSeparator = rowSeparator;
        this.splitter = Splitter.on(fieldSeparator);
    }
    
    public <RESULT> RESULT processRows(final EpfTableRowProcessor<ROW,RESULT> processor) throws IOException {
        return CharStreams.readLines(inputSupplier, lineProcessorForwardingTo(processor));
    }
    
    private <RESULT> LineProcessor<RESULT> lineProcessorForwardingTo(final EpfTableRowProcessor<ROW, RESULT> processor) {
        return new LineProcessor<RESULT>() {

            private List<String> buffer = Lists.newArrayList();
            
            public boolean processLine(String line) throws IOException {
                if(isComment(line)) {
                    return true;
                }
                if (rowSeparator == null) {
                    return processor.process(splitLineExtractor.apply(split(line)));
                }
                if(line.endsWith(rowSeparator)) {
                    ROW extracted = splitLineExtractor.apply(split(EMPTY_JOINER.join(Iterables.concat(buffer, ImmutableList.of(removeRowSeparator(line))))));
                    boolean continu = processor.process(extracted);
                    buffer.clear();
                    return continu;
                } else {
                    buffer.add(line+"\n");
                    return true;
                }
            }
            @Override
            public RESULT getResult() {
                return processor.getResult();
            }
        };
    }

    private String removeRowSeparator(String line) {
        int rsIndex = line.lastIndexOf(rowSeparator);
        if(rsIndex > 0) {
            return line.substring(0, rsIndex);
        }
        return line;
    }
    
    public List<String> split(String line) {
        return ImmutableList.copyOf(Iterables.transform(splitter.split(line), new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input.trim();
            }
        }));
    }
    
    private boolean isComment(String line) {
        return line.startsWith("#");
    }
}