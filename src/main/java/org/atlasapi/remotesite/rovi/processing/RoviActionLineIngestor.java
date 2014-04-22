package org.atlasapi.remotesite.rovi.processing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.Charset;

import org.atlasapi.media.entity.Content;
import org.atlasapi.remotesite.rovi.RoviContentWriter;
import org.atlasapi.remotesite.rovi.indexing.IndexAccessException;
import org.atlasapi.remotesite.rovi.indexing.KeyedActionLine;
import org.atlasapi.remotesite.rovi.parsers.RoviLineParser;

import com.google.common.base.Optional;


public abstract class RoviActionLineIngestor<T extends KeyedActionLine<?>, CONTENT extends Content> extends RoviLineProcessor<T> {

    private final RoviContentWriter contentWriter;

    public RoviActionLineIngestor(RoviLineParser<T> parser, Charset charset, RoviContentWriter contentWriter) {
        super(parser, charset);
        this.contentWriter = checkNotNull(contentWriter);
    }

    @Override
    protected void process(String line, T parsedLine) throws IndexAccessException {
        switch(parsedLine.getActionType()) {
            case INSERT:
                handleInsert(parsedLine);
                break;
                
            case UPDATE:
                handleUpdate(parsedLine);
                break;
                
            case DELETE:
                handleDelete(parsedLine);
                break;
                
            default:
                throw new RuntimeException("Unexpected action type: " + parsedLine.getActionType());
        }
    }

    private void handleInsert(T parsedLine) throws IndexAccessException {
        CONTENT content;
        content = createContent(parsedLine);
        contentWriter.writeContent(populateContent(content, parsedLine));
    }
    
    private void handleUpdate(T parsedLine) throws IndexAccessException {
        Optional<CONTENT> resolved = resolveContent(parsedLine);
        
        if (!resolved.isPresent()) {
            log().error("Received an update action of type {} for a not existent content with key {}",
                    parsedLine.getClass().getName(),
                    parsedLine.getKey());
            return;
        }
        
        CONTENT content = resolved.get();
        contentWriter.writeContent(populateContent(content, parsedLine));
    }
    
    private void handleDelete(T parsedLine) {
        Optional<CONTENT> resolved = resolveContent(parsedLine);
        
        if (!resolved.isPresent()) {
            log().info("Received a delete action of type {} for a not existent content with key {}",
                    parsedLine.getClass().getName(),
                    parsedLine.getKey());
            return;
        }
        
        CONTENT content = resolved.get();
        unpublishContent(content);
        contentWriter.writeContent(content);
    }
    
    protected abstract CONTENT populateContent(CONTENT content, T parsedLine) throws IndexAccessException;
    protected abstract Optional<CONTENT> resolveContent(T parsedLine);
    protected abstract CONTENT createContent(T parsedLine);
    

    private void unpublishContent(Content content) {
        content.setActivelyPublished(false);
    }

    @Override
    protected void doFinally(String line) {
        // Do nothing
    }
    
    @Override
    protected void handleBom() {
        // Do nothing
    }
    
    @Override
    protected void handleProcessingException(Exception e, String line) {
        // Swallow the exception and logs
        log().error(errorMessage(line), e);
    }
    
}
