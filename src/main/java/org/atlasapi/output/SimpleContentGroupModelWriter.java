package org.atlasapi.output;

import com.google.common.collect.Iterables;
import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.simple.ContentGroupQueryResult;
import org.atlasapi.output.simple.ContentGroupModelSimplifier;

import java.util.Set;

/**
 */
public class SimpleContentGroupModelWriter extends TransformingModelWriter<Iterable<ContentGroup>, ContentGroupQueryResult> {

    private final ContentGroupModelSimplifier simplifier;

    public SimpleContentGroupModelWriter(
            AtlasModelWriter<ContentGroupQueryResult> outputter,
            ContentGroupModelSimplifier simplifier
    ) {
        super(outputter);
        this.simplifier = simplifier;
    }

    @Override
    protected ContentGroupQueryResult transform(
            Iterable<ContentGroup> groups,
            final Set<Annotation> annotations,
            final Application application
    ) {
        ContentGroupQueryResult result = new ContentGroupQueryResult();
        result.setContentGroups(Iterables.transform(groups,
                input -> simplifier.simplify(input, annotations, application)));
        return result;
    }
}
