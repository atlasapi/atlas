package org.atlasapi.output.simple;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.output.Annotation;

import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class ContentGroupModelSimplifier extends DescribedModelSimplifier<org.atlasapi.media.entity.ContentGroup, org.atlasapi.media.entity.simple.ContentGroup> {

    private final SubstitutionTableNumberCodec codecForContent = SubstitutionTableNumberCodec.lowerCaseOnly();
    
    public ContentGroupModelSimplifier(ImageSimplifier imageSimplifier) {
        super(imageSimplifier);
    }
    
    @Override
    public org.atlasapi.media.entity.simple.ContentGroup simplify(org.atlasapi.media.entity.ContentGroup model, Set<Annotation> annotations, ApplicationConfiguration config) {

        org.atlasapi.media.entity.simple.ContentGroup simple = new org.atlasapi.media.entity.simple.ContentGroup();

        copyBasicDescribedAttributes(model, simple, annotations);

        simple.setContent(simpleContentListFrom(model.getContents()));
        simple.setType(model.getType().toString().toLowerCase());

        return simple;
    }

    private List<ContentIdentifier> simpleContentListFrom(Iterable<ChildRef> contents) {
        List<ContentIdentifier> contentList = Lists.newArrayList();
        for (ChildRef ref : contents) {
            contentList.add(ContentIdentifier.identifierFor(ref, codecForContent));
        }
        return contentList;
    }
}
