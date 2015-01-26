package org.atlasapi.input;

import java.util.Currency;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.segment.SegmentEvent;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Sets;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemModelTransformer extends ContentModelTransformer<org.atlasapi.media.entity.simple.Item, Item> {

    private final BroadcastModelTransformer broadcastTransformer;
    private final SegmentModelTransformer segmentModelTransformer;

    public ItemModelTransformer(LookupEntryStore lookupStore, TopicStore topicStore,
            ChannelResolver channelResolver, NumberToShortStringCodec idCodec,
            ClipModelTransformer clipsModelTransformer, Clock clock, SegmentModelTransformer segmentModelTransformer) {
        super(lookupStore, topicStore, idCodec, clipsModelTransformer, clock);
        this.broadcastTransformer = new BroadcastModelTransformer(channelResolver);
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
            Film film = new Film();
            item = film;
        } else if ("song".equals(type)) {
            item = createSong(inputItem);
        } else if ("broadcast".equals(type)) {
            item = createBroadcast(inputItem);
        } else {
            item = new Item();
        }
        item.setLastUpdated(now);
        return item;
    }

    private Item createBroadcast(org.atlasapi.media.entity.simple.Item inputItem) {
        Item item = new Item();
        HashSet<Broadcast> broadcasts = Sets.newHashSet();
        for(org.atlasapi.media.entity.simple.Broadcast broadcast : inputItem.getBroadcasts()) {
            broadcasts.add(broadcastTransformer.transform(broadcast));
        }
        Version version = new Version().copyWithBroadcasts(broadcasts);
        item.setVersions(ImmutableSet.of(version));
        return item;
    }

    private Item createSong(org.atlasapi.media.entity.simple.Item inputItem) {
        Song song = new Song();
        song.setIsrc(inputItem.getIsrc());
        if (inputItem.getDuration() != null) {
            song.setDuration(Duration.standardSeconds(inputItem.getDuration()));
        }
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
        Set<Encoding> encodings = encodingsFrom(inputItem.getLocations(), now);
        if (!encodings.isEmpty()) {
            Version version = new Version();
            version.setLastUpdated(now);
            version.setManifestedAs(encodings);
            item.setVersions(ImmutableSet.of(version));
        }
        if (inputItem.getBrandSummary() != null) {
            item.setParentRef(new ParentRef(inputItem.getBrandSummary().getUri()));
        }
        if (!inputItem.getBroadcasts().isEmpty()) {
            Set<Broadcast> broadcasts = Sets.newHashSet();
            for (org.atlasapi.media.entity.simple.Broadcast broadcast : inputItem.getBroadcasts()) {
                broadcasts.add(broadcastTransformer.transform(broadcast));
            }

            item.addVersion(new Version().copyWithBroadcasts(broadcasts));
        }
        if (inputItem != null && inputItem.getSegments() != null && !inputItem.getSegments().isEmpty()) {
            Set<SegmentEvent> segments = Sets.newHashSet();
            for (org.atlasapi.media.entity.simple.SegmentEvent segmentEvent : inputItem.getSegments()) {
                segments.add(segmentModelTransformer.transform(segmentEvent, inputItem.getPublisher()));
            }
            Version version = new Version();
            version.setSegmentEvents(segments);
            item.addVersion(version);
        }
        return item;
    }

    private Set<Encoding> encodingsFrom(Set<org.atlasapi.media.entity.simple.Location> locations, DateTime now) {
        Builder<Encoding> encodings = ImmutableSet.builder();
        for (org.atlasapi.media.entity.simple.Location simpleLocation : locations) {
            Encoding encoding = encodingFrom(simpleLocation, now);
            Location location = locationFrom(simpleLocation, now);
            Policy policy = policyFrom(simpleLocation, now);
            location.setPolicy(policy);
            encoding.setAvailableAt(ImmutableSet.of(location));
            encodings.add(encoding);
        }
        return encodings.build();
    }

    private Encoding encodingFrom(org.atlasapi.media.entity.simple.Location inputLocation, DateTime now) {
        Encoding encoding = new Encoding();
        encoding.setLastUpdated(now);
        encoding.setAdvertisingDuration(inputLocation.getAdvertisingDuration());
        encoding.setAudioBitRate(inputLocation.getAudioBitRate());
        encoding.setAudioChannels(inputLocation.getAudioChannels());
        encoding.setAudioCoding(asMimeType(inputLocation.getAudioCoding()));
        encoding.setBitRate(inputLocation.getBitRate());
        encoding.setContainsAdvertising(inputLocation.getContainsAdvertising());
        encoding.setDataContainerFormat(asMimeType(inputLocation.getDataContainerFormat()));
        encoding.setDataSize(inputLocation.getDataSize());
        encoding.setDistributor(inputLocation.getDistributor());
        encoding.setHasDOG(inputLocation.getHasDOG());
        encoding.setSource(inputLocation.getSource());
        encoding.setVideoAspectRatio(inputLocation.getVideoAspectRatio());
        encoding.setVideoBitRate(inputLocation.getVideoBitRate());
        encoding.setVideoCoding(asMimeType(inputLocation.getVideoCoding()));
        encoding.setVideoFrameRate(inputLocation.getVideoFrameRate());
        encoding.setVideoHorizontalSize(inputLocation.getVideoHorizontalSize());
        encoding.setVideoProgressiveScan(inputLocation.getVideoProgressiveScan());
        encoding.setVideoVerticalSize(inputLocation.getVideoVerticalSize());
        return encoding;
    }

    private Location locationFrom(org.atlasapi.media.entity.simple.Location inputLocation, DateTime now) {
        Location location = new Location();
        location.setLastUpdated(now);
        location.setEmbedCode(inputLocation.getEmbedCode());
        location.setEmbedId(inputLocation.getEmbedId());
        location.setTransportIsLive(inputLocation.getTransportIsLive());
        location.setUri(inputLocation.getUri());

        if (inputLocation.getTransportSubType() != null) {
            location.setTransportSubType(TransportSubType.fromString(inputLocation.getTransportSubType()));
        }
        if (inputLocation.getTransportType() != null) {
            location.setTransportType(TransportType.fromString(inputLocation.getTransportType()));
        }
        return location;
    }

    private Policy policyFrom(org.atlasapi.media.entity.simple.Location inputLocation, DateTime now) {
        Policy policy = new Policy();
        policy.setLastUpdated(now);
        policy.setAvailabilityStart(asUtcDateTime(inputLocation.getAvailabilityStart()));
        policy.setAvailabilityEnd(asUtcDateTime(inputLocation.getAvailabilityEnd()));
        policy.setDrmPlayableFrom(asUtcDateTime(inputLocation.getDrmPlayableFrom()));
        policy.setPlatform(Platform.fromKey(inputLocation.getPlatform()));
        if (inputLocation.getCurrency() != null && inputLocation.getPrice() != null) {
            Currency currency = Currency.getInstance(inputLocation.getCurrency());
            policy.setPrice(new Price(currency, inputLocation.getPrice()));
        }
        if (inputLocation.getAvailableCountries() != null) {
            policy.setAvailableCountries(Countries.fromCodes(inputLocation.getAvailableCountries()));
        }
        policy.setRevenueContract(RevenueContract.fromKey(inputLocation.getRevenueContract()));
        return policy;
    }

    private MimeType asMimeType(String mimeType) {
        if (mimeType != null) {
            return MimeType.fromString(mimeType);
        }
        return null;
    }

    private DateTime asUtcDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return new DateTime(date).withZone(DateTimeZones.UTC);
    }

}
