package org.atlasapi.input;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.Clock;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.segment.SegmentEvent;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.LocalDate;

import java.util.Date;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemModelTransformer extends ContentModelTransformer<org.atlasapi.media.entity.simple.Item, Item> {

    private final BroadcastModelTransformer broadcastTransformer;
    private final EncodingModelTransformer encodingTransformer;
    private final RestrictionModelTransformer restrictionTransformer;
    private final SegmentModelTransformer segmentModelTransformer;

    public ItemModelTransformer(LookupEntryStore lookupStore, TopicStore topicStore,
            ChannelResolver channelResolver, NumberToShortStringCodec idCodec,
            ClipModelTransformer clipsModelTransformer, Clock clock,
            SegmentModelTransformer segmentModelTransformer,
            ImageModelTransformer imageModelTransformer) {

        super(lookupStore, topicStore, idCodec, clipsModelTransformer, clock, imageModelTransformer);
        this.broadcastTransformer = BroadcastModelTransformer.create(channelResolver);
        this.encodingTransformer = EncodingModelTransformer.create();
        this.restrictionTransformer = RestrictionModelTransformer.create();
        this.segmentModelTransformer = checkNotNull(segmentModelTransformer);
    }

    @Override
    protected Item createOutput(org.atlasapi.media.entity.simple.Item inputItem) {
        DateTime now = this.clock.now();
        String type = inputItem.getType();
        Item item;
        if ("episode".equals(type)) {
            item = createEpisode(inputItem);
        } else if ("film".equals(type)) {
            item = createFilm(inputItem);
        } else if ("song".equals(type)) {
            item = createSong(inputItem);
        } else {
            item = new Item();
        }
        item.setLastUpdated(now);
        return item;
    }

    private Item createFilm(org.atlasapi.media.entity.simple.Item inputItem) {
        Film film = new Film();
        film.setYear(inputItem.getYear());
        film.setReleaseDates(releaseDatesFrom(inputItem.getReleaseDates()));
        return film;
    }

    private Item createSong(org.atlasapi.media.entity.simple.Item inputItem) {
        Song song = new Song();
        song.setIsrc(inputItem.getIsrc());
        return song;
    }

    private Item createEpisode(org.atlasapi.media.entity.simple.Item inputItem) {
        Episode episode = new Episode();
        episode.setSeriesNumber(inputItem.getSeriesNumber());
        episode.setEpisodeNumber(inputItem.getEpisodeNumber());

        if (inputItem.getSeriesSummary() != null) {
            episode.setSeriesRef(new ParentRef(inputItem.getSeriesSummary().getUri()));
        }
        return episode;
    }

    @Override
    protected Item setFields(Item item, org.atlasapi.media.entity.simple.Item inputItem) {
        super.setFields(item, inputItem);
        DateTime now = this.clock.now();

        Version version = new Version();

        Set<Encoding> encodings = encodingsFrom(inputItem.getLocations(), now);
        if (!encodings.isEmpty()) {
            version.setLastUpdated(now);
            version.setManifestedAs(encodings);
        }

        if (inputItem.getBrandSummary() != null) {
            item.setParentRef(new ParentRef(inputItem.getBrandSummary().getUri()));
        }

        if (!inputItem.getBroadcasts().isEmpty()) {
            version.setLastUpdated(now);
            addBroadcasts(inputItem, version);
        }

        if (inputItem.getSegments() != null && !inputItem.getSegments().isEmpty()) {
            Set<SegmentEvent> segments = Sets.newHashSet();
            for (org.atlasapi.media.entity.simple.SegmentEvent segmentEvent : inputItem.getSegments()) {
                segments.add(segmentModelTransformer.transform(segmentEvent, inputItem.getPublisher()));
            }
            version.setLastUpdated(now);
            version.setSegmentEvents(segments);
        }

        addRestrictions(inputItem, version);

        item.setYear(inputItem.getYear());
        item.setCountriesOfOrigin(inputItem.getCountriesOfOrigin());

        if (inputItem.getDuration() != null) {
            item.setDuration(Duration.millis(inputItem.getDuration()));
            version.setDuration(Duration.millis(inputItem.getDuration()));
        } else {
            checkAndSetConsistentDurationFromLocations(inputItem, version);
        }

        item.setVersions(ImmutableSet.of(version));

        return item;
    }

    private void checkAndSetConsistentDurationFromLocations(
            org.atlasapi.media.entity.simple.Item item,
            Version version
    ) {

        Set<Integer> durations = Sets.newHashSet();
        for (org.atlasapi.media.entity.simple.Location location : item.getLocations()) {
            Integer duration = location.getDuration();
            if (duration != null) {
                durations.add(duration);
            }
        }
        if (durations.size() > 1 ) {
            throw new IllegalArgumentException("Locations for " + item.getUri() + " have inconsistent durations");
        } else if (durations.size() == 1) {
            Duration duration = new Duration(Iterables.getOnlyElement(durations).longValue());
            version.setDuration(duration);
        }
    }

    private void addBroadcasts(org.atlasapi.media.entity.simple.Item inputItem, Version version) {
        Set<Broadcast> broadcasts = Sets.newHashSet();

        for (org.atlasapi.media.entity.simple.Broadcast broadcast : inputItem.getBroadcasts()) {
            broadcasts.add(broadcastTransformer.transform(broadcast));
        }

        version.setBroadcasts(broadcasts);
    }
    
    private void addRestrictions(org.atlasapi.media.entity.simple.Item inputItem, Version version) {
        // Since we are coalescing multiple broadcasts each with possibly its own restriction there is
        // no good way decide which restriction to keep so we are keeping the first one
        for (org.atlasapi.media.entity.simple.Broadcast broadcast : inputItem.getBroadcasts()) {
            Optional<Restriction> restriction = createRestriction(broadcast.getRestriction());

            if (restriction.isPresent()) {
                version.setRestriction(restriction.get());
                return;
            }
        }

        for (org.atlasapi.media.entity.simple.Location location : inputItem.getLocations()) {
            Optional<Restriction> restriction = createRestriction(location.getRestriction());

            if (restriction.isPresent()) {
                version.setRestriction(restriction.get());
                return;
            }
        }
    }

    private Optional<Restriction> createRestriction(
            org.atlasapi.media.entity.simple.Restriction simpleRestriction
    ) {
        if (simpleRestriction == null) {
            return Optional.absent();
        }

        Restriction restriction = restrictionTransformer.transform(simpleRestriction);

        return Optional.of(restriction);
    }

    private Set<Encoding> encodingsFrom(Set<org.atlasapi.media.entity.simple.Location> locations, DateTime now) {
        Builder<Encoding> encodings = ImmutableSet.builder();
        for (org.atlasapi.media.entity.simple.Location simpleLocation : locations) {
            Encoding encoding = encodingTransformer.transform(simpleLocation, now);
            encodings.add(encoding);
        }
        return encodings.build();
    }

    private Set<ReleaseDate> releaseDatesFrom(Set<org.atlasapi.media.entity.simple.ReleaseDate> releaseDates) {
        ImmutableSet.Builder<ReleaseDate> builder = new ImmutableSet.Builder<>();

        for (org.atlasapi.media.entity.simple.ReleaseDate releaseDate : releaseDates) {
            ReleaseDate complexReleaseDate = new ReleaseDate(convertToLocalDate(releaseDate.getDate()),
                    Countries.fromCode(releaseDate.getCountry()),
                    ReleaseDate.ReleaseType.valueOf(releaseDate.getType().toUpperCase()));
            builder.add(complexReleaseDate);
        }
        return builder.build();
    }

    private LocalDate convertToLocalDate(Date date) {
        if(date == null) {
            return null;
        }
        return new LocalDate(date);
    }

}
