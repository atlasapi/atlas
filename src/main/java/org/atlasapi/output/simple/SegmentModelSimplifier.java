package org.atlasapi.output.simple;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentEvent;
import org.atlasapi.media.segment.SegmentRef;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class SegmentModelSimplifier implements
        ModelSimplifier<List<SegmentEvent>, List<org.atlasapi.media.entity.simple.SegmentEvent>> {

    private final SegmentResolver segmentResolver;
    private final NumberToShortStringCodec codec = new SubstitutionTableNumberCodec();

    public SegmentModelSimplifier(SegmentResolver segmentResolver) {
        this.segmentResolver = segmentResolver;
    }

    @Override
    public List<org.atlasapi.media.entity.simple.SegmentEvent> simplify(
            List<SegmentEvent> segmentEvents, Set<Annotation> annotations,
            ApplicationConfiguration config) {
        final Map<SegmentRef, Maybe<Segment>> resolvedSegs = segmentResolver.resolveById(
                ImmutableSet.copyOf(Lists.transform(segmentEvents, SegmentEvent.TO_REF)));
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(segmentEvents,
                new Function<SegmentEvent, org.atlasapi.media.entity.simple.SegmentEvent>() {

                    @Override
                    public org.atlasapi.media.entity.simple.SegmentEvent apply(SegmentEvent input) {
                        Maybe<Segment> segment = resolvedSegs.get(input.getSegment());
                        if (segment.hasValue()) {
                            return simplify(input, segment.requireValue());
                        }
                        return null;
                    }
                }), Predicates.notNull()));
    }

    private org.atlasapi.media.entity.simple.SegmentEvent simplify(SegmentEvent event,
            Segment segment) {
        final org.atlasapi.media.entity.simple.SegmentEvent segmentEvent = new org.atlasapi.media.entity.simple.SegmentEvent();

        final org.atlasapi.media.entity.Description description = event.getDescription();
        segmentEvent.setTitle(description.getTitle());
        segmentEvent.setDescription(description.getSynopsis());

        segmentEvent.setUri(event.getCanonicalUri());
        segmentEvent.setIsChapter(event.getIsChapter());
        segmentEvent.setPosition(event.getPosition());
        if (event.getOffset() != null) {
            segmentEvent.setOffset(Ints.saturatedCast(event.getOffset().getStandardSeconds()));
        }
        segmentEvent.setSegment(simplify(segment));

        return segmentEvent;
    }

    private org.atlasapi.media.entity.simple.Segment simplify(Segment segment) {
        final org.atlasapi.media.entity.simple.Segment seg = new org.atlasapi.media.entity.simple.Segment();

        seg.setUri(segment.getCanonicalUri());
        seg.setId(codec.encode(BigInteger.valueOf(segment.getId())));

        seg.setLongDescription(segment.getLongDescription());
        seg.setMediumDescription(segment.getMediumDescription());
        seg.setShortDescription(segment.getShortDescription());
        seg.setTitle(segment.getTitle());
        seg.setDescription(segment.getDescription());

        if (seg.getDuration() != null) {
            seg.setDuration(Ints.saturatedCast(segment.getDuration().getStandardSeconds()));
        }
        if (segment.getType() != null) {
            seg.setSegmentType(segment.getType().toString());
        }

        return seg;
    }
}
