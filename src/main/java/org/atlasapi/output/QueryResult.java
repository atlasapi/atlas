package org.atlasapi.output;

import com.google.common.base.Optional;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.media.entity.Identified;

import javax.annotation.Nullable;
import java.util.Iterator;


public class QueryResult<CONTENT, CONTEXT> implements Iterable<CONTENT> {
    
    public static <CONTENT extends Identified, CONTEXT extends Identified> QueryResult<CONTENT, CONTEXT> of(Iterable<CONTENT> content) {
        return of(content, null);
    }

    public static <CONTENT extends Identified, CONTEXT extends Identified> QueryResult<CONTENT, CONTEXT> of(Iterable<CONTENT> content, @Nullable CONTEXT context) {
        return new QueryResult<CONTENT, CONTEXT>(content, Optional.fromNullable(context));
    }

    private final Iterable<CONTENT> content;
    private final Optional<CONTEXT> context;
    private Selection selection;

    QueryResult(Iterable<CONTENT> content, Optional<CONTEXT> context) {
        this.content = content;
        this.context = context;
    }

    public Optional<CONTEXT> getContext() {
        return context;
    }

    public Iterable<CONTENT> getContent() {
        return content;
    }

    @Override
    public Iterator<CONTENT> iterator() {
        return content.iterator();
    }
    
    public QueryResult<CONTENT,CONTEXT> withSelection(Selection selection) {
        this.selection = selection;
        return this;
    }
    
    public Selection getSelection() {
        return selection;
    }
}
