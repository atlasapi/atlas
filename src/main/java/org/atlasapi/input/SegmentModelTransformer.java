package org.atlasapi.input;


import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import org.atlasapi.media.SegmentType;
import org.atlasapi.media.entity.Description;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentEvent;
import org.atlasapi.media.segment.SegmentRef;
import org.atlasapi.media.segment.SegmentWriter;
import org.joda.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;


public class SegmentModelTransformer {

    private final SegmentWriter segmentWriter;
    private final NumberToShortStringCodec codec = new SubstitutionTableNumberCodec(); 
    
    public SegmentModelTransformer(SegmentWriter segmentWriter) {
        this.segmentWriter = checkNotNull(segmentWriter);
    }

    public SegmentEvent transform(org.atlasapi.media.entity.simple.SegmentEvent simple, PublisherDetails publisher) {
        checkNotNull(simple.getSegment() != null, "You must specify a Segment on the SegmentEvent");
        SegmentEvent complex = new SegmentEvent();
        complex.setCanonicalUri(simple.getUri());
        complex.setDescription(Description.description()
                .withSynopsis(simple.getDescription())
                .withTitle(simple.getTitle())
                .build());
        complex.setPosition(simple.getPosition());
        if (simple.getOffset() != null) {
            complex.setOffset(Duration.standardSeconds(simple.getOffset()));
        }
        complex.setIsChapter(simple.getIsChapter());
        if (Strings.isNullOrEmpty(simple.getSegment().getId())) {
            complex.setSegment(writeSegment(simple.getSegment(), publisher));
        } else {
            complex.setSegment(new SegmentRef(codec.decode(simple.getSegment().getId()).longValue()));
        }
        return complex;
    }

    private SegmentRef writeSegment(org.atlasapi.media.entity.simple.Segment segment, PublisherDetails publisher) {
        Segment complex = transform(segment, publisher);
        long id = segmentWriter.write(complex).getId();
        return new SegmentRef(id);
    }

    private Segment transform(org.atlasapi.media.entity.simple.Segment simple, PublisherDetails publisher) {
        checkArgument((simple.getDuration() != null && simple.getDuration() > 0),
                "You must specify a (positive) duration on the Segment");
        checkArgument(!Strings.isNullOrEmpty(simple.getSegmentType()),
                "You must specify a type of Segment");

        Segment complex = new Segment();
        complex.setPublisher(Publisher.fromKey(publisher.getKey()).requireValue());
        complex.setType(SegmentType.valueOf(simple.getSegmentType()));
        complex.setDuration(Duration.standardSeconds(simple.getDuration()));
        complex.setDescription(simple.getDescription());
        complex.setRelatedLinks(relatedLinks(simple.getRelatedLinks()));
        complex.setTitle(simple.getTitle());
        return complex;
    }

    private Iterable<RelatedLink> relatedLinks(
            List<org.atlasapi.media.entity.simple.RelatedLink> relatedLinks) {
        return Lists.transform(relatedLinks,
                new Function<org.atlasapi.media.entity.simple.RelatedLink, RelatedLink>() {
                    @Override
                    public RelatedLink apply(org.atlasapi.media.entity.simple.RelatedLink input) {
                        RelatedLink.LinkType type = RelatedLink.LinkType.valueOf(input.getType().toUpperCase());
                        RelatedLink.Builder link = RelatedLink.relatedLink(type, input.getUrl())
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
}
