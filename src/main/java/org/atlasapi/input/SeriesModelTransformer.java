package org.atlasapi.input;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesModelTransformer extends ContentModelTransformer<Playlist, Series> {

    private final EncodingModelTransformer encodingTransformer;
    private final RestrictionModelTransformer restrictionTransformer;

    public SeriesModelTransformer(LookupEntryStore lookupStore,
            TopicStore topicStore,
            NumberToShortStringCodec idCodec,
            ClipModelTransformer clipsModelTransformer, Clock clock) {
        super(lookupStore, topicStore, idCodec, clipsModelTransformer, clock);
        this.encodingTransformer = EncodingModelTransformer.create();
        this.restrictionTransformer = RestrictionModelTransformer.create();
    }

    @Override
    protected Series createOutput(Playlist simple) {
        checkNotNull(simple.getUri(),
                "Cannot create a Series from simple Playlist without URI");
        checkNotNull(simple.getPublisher(),
                "Cannot create a Series from simple Playlist without a Publisher");
        checkNotNull(simple.getPublisher().getKey(),
                "Cannot create a Series from simple Playlist without a Publisher key");

        Series series = new Series(
                simple.getUri(),
                simple.getCurie(),
                Publisher.fromKey(simple.getPublisher().getKey()).requireValue()
        );
        return series;

    }

    @Override
    protected Series setFields(Series result, Playlist simple) {
        super.setFields(result, simple);
        DateTime now = this.clock.now();
        result.setTotalEpisodes(simple.getTotalEpisodes());
        result.withSeriesNumber(simple.getSeriesNumber());
        if (simple.getBrandSummary() != null) {
            result.setParentRef(new ParentRef(simple.getBrandSummary().getUri()));
        }
        result.setCountriesOfOrigin(simple.getCountriesOfOrigin());

        Version version = new Version();
        Set<Encoding> encodings = encodingsFrom(simple.getLocations(), now);
        if (!encodings.isEmpty()) {
            version.setLastUpdated(now);
            version.setManifestedAs(encodings);
        }
        addRestrictions(simple, version);
        result.setVersions(ImmutableSet.of(version));

        return result;
    }

    private void addRestrictions(org.atlasapi.media.entity.simple.Playlist simple, Version version) {
        // Since we are coalescing multiple locations each with possibly its own restriction there is
        // no good way decide which restriction to keep so we are keeping the first one
        for (org.atlasapi.media.entity.simple.Location location : simple.getLocations()) {
            if (location.getRestriction() != null) {
                Restriction restriction = restrictionTransformer.transform(location.getRestriction());
                version.setRestriction(restriction);
                return;
            }
        }
    }

    private Set<Encoding> encodingsFrom(Set<org.atlasapi.media.entity.simple.Location> locations, DateTime now) {
        ImmutableSet.Builder<Encoding> encodings = ImmutableSet.builder();
        for (org.atlasapi.media.entity.simple.Location simpleLocation : locations) {
            Encoding encoding = encodingTransformer.transform(simpleLocation, now);
            encodings.add(encoding);
        }
        return encodings.build();
    }
}
