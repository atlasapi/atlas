package org.atlasapi.input;

import java.util.List;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.RelatedLink.Builder;
import org.atlasapi.media.entity.RelatedLink.LinkType;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;

public abstract class DescribedModelTransformer<F extends Description,T extends Described> implements ModelTransformer<F, T> {

    private Clock clock;

    public DescribedModelTransformer(Clock clock) {
        this.clock = clock;
    }
    
    @Override
    public final T transform(F simple) {
        DateTime now = clock.now();
        T output = createDescribedOutput(simple, now);
        output.setLastUpdated(now);
        return setDescriptionFields(output, simple);
    }

    private T setDescriptionFields(T result, F inputContent) {
        result.setCanonicalUri(inputContent.getUri());
        result.setCurie(inputContent.getCurie());
        Publisher publisher = getPublisher(inputContent.getPublisher());
        result.setPublisher(publisher);
        result.setTitle(inputContent.getTitle());
        result.setDescription(inputContent.getDescription());
        result.setImage(inputContent.getImage());
        result.setThumbnail(inputContent.getThumbnail());
        result.setRelatedLinks(relatedLinks(inputContent.getRelatedLinks()));
        if (inputContent.getSpecialization() != null) {
            result.setSpecialization(Specialization.fromKey(inputContent.getSpecialization()).valueOrNull());
        }
        if (inputContent.getMediaType() != null) {
            result.setMediaType(MediaType.valueOf(inputContent.getMediaType().toUpperCase()));
        }
        return result;
    }

    private Iterable<RelatedLink> relatedLinks(
            List<org.atlasapi.media.entity.simple.RelatedLink> relatedLinks) {
        return Lists.transform(relatedLinks,
            new Function<org.atlasapi.media.entity.simple.RelatedLink, RelatedLink>() {
                @Override
                public RelatedLink apply(org.atlasapi.media.entity.simple.RelatedLink input) {
                    LinkType type = LinkType.valueOf(input.getType().toUpperCase());
                    Builder link = RelatedLink.relatedLink(type,input.getUrl())
                       .withSourceId(input.getSourceId())
                       .withShortName(input.getShortName())
                       .withTitle(input.getTitle())
                       .withDescription(input.getDescription())
                       .withImage(input.getImage())
                       .withThumbnail(input.getThumbnail());
                    return link.build();
                }
            }
        );
    }

    protected Publisher getPublisher(PublisherDetails pubDets) {
        if (pubDets == null || pubDets.getKey() == null) {
            throw new IllegalArgumentException("missing publisher");
        }
        Maybe<Publisher> possiblePublisher = Publisher.fromKey(pubDets.getKey());
        if (possiblePublisher.isNothing()) {
            throw new IllegalArgumentException("unknown publisher " + pubDets.getKey());
        }
        return possiblePublisher.requireValue();
    }
    
    protected abstract T createDescribedOutput(F simple, DateTime now);
    
}
