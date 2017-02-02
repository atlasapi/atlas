package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.simple.ContentGroupQueryResult;
import org.atlasapi.output.simple.ContentGroupModelSimplifier;

/**
 */
public class SimpleContentGroupModelWriter extends TransformingModelWriter<Iterable<ContentGroup>, ContentGroupQueryResult> {

    private final ContentGroupModelSimplifier simplifier;

    public SimpleContentGroupModelWriter(AtlasModelWriter<ContentGroupQueryResult> outputter, ContentGroupModelSimplifier simplifier) {
        super(outputter);
        this.simplifier = simplifier;
    }

    @Override
    protected ContentGroupQueryResult transform(Iterable<ContentGroup> groups, final Set<Annotation> annotations, final ApplicationConfiguration config) {
        ContentGroupQueryResult result = new ContentGroupQueryResult();
        result.setContentGroups(Iterables.transform(groups, new Function<ContentGroup, org.atlasapi.media.entity.simple.ContentGroup>() {

            @Override
            public org.atlasapi.media.entity.simple.ContentGroup apply(ContentGroup input) {
                return simplifier.simplify(input, annotations, config);
            }
        }));
        return result;
    }
}
