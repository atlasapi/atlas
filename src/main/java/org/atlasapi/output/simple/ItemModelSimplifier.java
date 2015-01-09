package org.atlasapi.output.simple;

import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.utils.DescriptionWatermarker;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Player;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.Service;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.BlackoutRestriction;
import org.atlasapi.media.entity.simple.BrandSummary;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.Identified;
import org.atlasapi.media.entity.simple.Language;
import org.atlasapi.media.entity.simple.Restriction;
import org.atlasapi.media.entity.simple.SeriesSummary;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.output.AvailableItemsResolver;
import org.atlasapi.persistence.output.ContainerSummaryResolver;
import org.atlasapi.persistence.output.UpcomingItemsResolver;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;

public class ItemModelSimplifier extends ContentModelSimplifier<Item, org.atlasapi.media.entity.simple.Item> {

    private static final Logger log = LoggerFactory.getLogger(ItemModelSimplifier.class);

    // Parent channels don't have images currently, so we override the images for them
    // from chosen child channels. This map is from parent channel -> child channel
    // where the child channel's images are provided.
    
    // For ease of maintenance, we keep the map as strings, then map to Long ids at runtime
    private static final Map<String, String> CHANNEL_IMAGE_OVERRIDES = ImmutableMap.<String, String>builder()
                                                                               .put("cbPF", "cbbh") // BBC One
                                                                               .put("cbRD", "cbbG") // BBC Two
                                                                               .put("cbSM", "cbdw") // ITV1
                                                                               .put("cbPG", "cbdj") // Channel4
                                                                               .put("cbSR", "cbns") // Sky Atlantic
                                                                               .put("cbPK", "cbdq") // Channel 5
                                                                               .put("cbPW", "cbhf") // Sky 1 
                                                                               .put("cbRT", "cbny") // Sky living
                                                                               .put("cbQm", "cbdZ")// ITV2
                                                                               .put("cbRN", "cbd2")// ITV3
                                                                               .put("cbRX", "cbd5")// ITV4
                                                                               .put("cbWw", "cbbG")// BBC Two HD
                                                                               .put("cbj8", "cbhs") // Sky Sports 1 HD
                                                                               .put("cbj9", "cbht") // Sky Sports 2 HD
                                                                               .build();
   
    private final NumberToShortStringCodec idCodec;
    private final ContainerSummaryResolver containerSummaryResolver;
    private final ChannelResolver channelResolver;
    private final Clock clock;
    private final SegmentModelSimplifier segmentSimplifier;
    private final NumberToShortStringCodec channelIdCodec;
    private final ImageSimplifier imageSimplifier;
    private final Map<Long, Long> channelImageOverrides;
    private final ServiceModelSimplifier serviceModelSimplifier;
    private final PlayerModelSimplifier playerModelSimplifier;
    private final ServiceResolver serviceResolver;
    private final PlayerResolver playerResolver;
    
    public ItemModelSimplifier(String localHostName, ContentGroupResolver contentGroupResolver, 
            TopicQueryResolver topicResolver, ProductResolver productResolver, SegmentResolver segmentResolver, 
            ContainerSummaryResolver containerSummaryResolver, ChannelResolver channelResolver, 
            NumberToShortStringCodec idCodec, NumberToShortStringCodec channelIdCodec, 
            ImageSimplifier imageSimplifier, PeopleQueryResolver personResolver, UpcomingItemsResolver upcomingResolver, 
            AvailableItemsResolver availableResolver, @Nullable DescriptionWatermarker descriptionWatermarker,
            PlayerResolver playerResolver, PlayerModelSimplifier playerModelSimplifier, 
            ServiceResolver serviceResolver, ServiceModelSimplifier serviceModelSimplifier, EventRefModelSimplifier eventSimplifier) {
        this(localHostName, contentGroupResolver, topicResolver, productResolver, segmentResolver, 
                containerSummaryResolver, channelResolver, idCodec, channelIdCodec, new SystemClock(), 
                imageSimplifier, personResolver, upcomingResolver, availableResolver, descriptionWatermarker,
                playerResolver, playerModelSimplifier, serviceResolver, serviceModelSimplifier, eventSimplifier);
    }

    public ItemModelSimplifier(String localHostName, ContentGroupResolver contentGroupResolver, 
            TopicQueryResolver topicResolver, ProductResolver productResolver, SegmentResolver segmentResolver, 
            ContainerSummaryResolver containerSummaryResolver, ChannelResolver channelResolver, 
            NumberToShortStringCodec idCodec, NumberToShortStringCodec channelIdCodec, Clock clock, 
            ImageSimplifier imageSimplifier, PeopleQueryResolver personResolver, UpcomingItemsResolver upcomingResolver, 
            AvailableItemsResolver availableResolver, @Nullable DescriptionWatermarker descriptionWatermarker,
            PlayerResolver playerResolver, PlayerModelSimplifier playerModelSimplifier, 
            ServiceResolver serviceResolver, ServiceModelSimplifier serviceModelSimplifier, EventRefModelSimplifier eventSimplifier) {
        
        super(localHostName, contentGroupResolver, topicResolver, productResolver, imageSimplifier, 
                personResolver, upcomingResolver, availableResolver, descriptionWatermarker, eventSimplifier);
        
        this.containerSummaryResolver = containerSummaryResolver;
        this.clock = clock;
        this.imageSimplifier = imageSimplifier;
        this.segmentSimplifier = segmentResolver != null ? new SegmentModelSimplifier(segmentResolver) : null;
        this.channelResolver = channelResolver;
        this.idCodec = idCodec;
        this.channelIdCodec = channelIdCodec;
        this.serviceModelSimplifier = serviceModelSimplifier;
        this.playerModelSimplifier = playerModelSimplifier;
        this.serviceResolver = serviceResolver; //TODO checkNotNull(serviceResolver);
        this.playerResolver = playerResolver; //TODO checkNotNull(playerResolver);
        ImmutableMap.Builder<Long, Long> builder = ImmutableMap.builder();
        
        for (Entry<String, String> entry : CHANNEL_IMAGE_OVERRIDES.entrySet()) {
            builder.put(channelIdCodec.decode(entry.getKey()).longValue(), channelIdCodec.decode(entry.getValue()).longValue());
        }
        this.channelImageOverrides = builder.build();
    }

    @Override
    public org.atlasapi.media.entity.simple.Item simplify(Item full, final Set<Annotation> annotations, final ApplicationConfiguration config) {

        org.atlasapi.media.entity.simple.Item simple = new org.atlasapi.media.entity.simple.Item();

        copyProperties(full, simple, annotations, config);

        boolean doneSegments = false;
        for (Version version : full.getVersions()) {
            addTo(simple, version, full, annotations, config);
            if (!doneSegments && !version.getSegmentEvents().isEmpty() && annotations.contains(Annotation.SEGMENT_EVENTS) && segmentSimplifier != null) {
                simple.setSegments(segmentSimplifier.simplify(version.getSegmentEvents(), annotations, config));
                doneSegments = true;
            }
        }

        return simple;
    }

    private void copyProperties(Item fullItem, org.atlasapi.media.entity.simple.Item simpleItem, Set<Annotation> annotations, ApplicationConfiguration config) {
        copyBasicContentAttributes(fullItem, simpleItem, annotations, config);
        simpleItem.setType(EntityType.from(fullItem).toString());

        if (annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            simpleItem.setBlackAndWhite(fullItem.getBlackAndWhite());
            simpleItem.setCountriesOfOrigin(fullItem.getCountriesOfOrigin());
            simpleItem.setScheduleOnly(fullItem.isScheduleOnly());
        }

        if (fullItem.getContainer() != null) {
            simpleItem.setBrandSummary(summaryFromResolved(fullItem.getContainer(), annotations));
        }

        if (fullItem instanceof Episode) {
            Episode episode = (Episode) fullItem;

            if (annotations.contains(Annotation.DESCRIPTION) || annotations.contains(Annotation.EXTENDED_DESCRIPTION) || annotations.contains(Annotation.SERIES_SUMMARY)) {
                ParentRef series = episode.getSeriesRef();
                if (series != null) {
                    simpleItem.setSeriesSummary(seriesSummaryFromResolved(series, annotations));
                }
            }
            
            if (annotations.contains(Annotation.DESCRIPTION)) {
                simpleItem.setSpecial(episode.getSpecial());
            }
            
            if (annotations.contains(Annotation.DESCRIPTION) || annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
                simpleItem.setEpisodeNumber(episode.getEpisodeNumber());
            }

            if (annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
                simpleItem.setSeriesNumber(episode.getSeriesNumber());
            }

        } else if (fullItem instanceof Film && annotations.contains(Annotation.EXTENDED_DESCRIPTION)) {
            Film film = (Film) fullItem;
            simpleItem.setSubtitles(simpleSubtitlesFrom(film.getSubtitles()));
            simpleItem.setReleaseDates(simpleReleaseDate(film.getReleaseDates()));
        } else if (fullItem instanceof Song) {
            Song song = (Song) fullItem;
            simpleItem.setIsrc(song.getIsrc());
            if (song.getDuration() != null) {
                simpleItem.setDuration(song.getDuration().getStandardSeconds());
            }
        }
    }

    private Iterable<org.atlasapi.media.entity.simple.ReleaseDate> simpleReleaseDate(Set<ReleaseDate> releaseDates) {
        return Iterables.transform(releaseDates, new Function<ReleaseDate, org.atlasapi.media.entity.simple.ReleaseDate>() {

            @Override
            public org.atlasapi.media.entity.simple.ReleaseDate apply(ReleaseDate input) {
                return new org.atlasapi.media.entity.simple.ReleaseDate(
                        input.date().toDateTimeAtStartOfDay(DateTimeZones.UTC).toDate(),
                        input.country().code(),
                        input.type().toString().toLowerCase());
            }
        });
    }

    private Iterable<org.atlasapi.media.entity.simple.Subtitles> simpleSubtitlesFrom(Set<Subtitles> subtitles) {
        return Iterables.filter(Iterables.transform(subtitles, new Function<Subtitles, org.atlasapi.media.entity.simple.Subtitles>() {

            @Override
            public org.atlasapi.media.entity.simple.Subtitles apply(Subtitles input) {
                Language lang = languageForCode(input.code());
                return lang == null ? null : new org.atlasapi.media.entity.simple.Subtitles(lang);
            }
        }), Predicates.notNull());
    }

    private void addTo(org.atlasapi.media.entity.simple.Item simpleItem, Version version, Item item, Set<Annotation> annotations, ApplicationConfiguration config) {

        if (annotations.contains(Annotation.LOCATIONS) || annotations.contains(Annotation.AVAILABLE_LOCATIONS)) {
            for (Encoding encoding : version.getManifestedAs()) {
                addTo(simpleItem, version, encoding, item, annotations, config);
            }
        }

        Iterable<Broadcast> broadcasts = null;
        if (annotations.contains(Annotation.BROADCASTS)) {
            broadcasts = filterInactive(version.getBroadcasts());
        } else if (annotations.contains(Annotation.FIRST_BROADCASTS)) {
            broadcasts = firstBroadcasts(filterInactive(version.getBroadcasts()));
        } else if (annotations.contains(Annotation.NEXT_BROADCASTS)) {
            broadcasts = nextBroadcast(filterInactive(version.getBroadcasts()));
        }
        if (broadcasts != null) {
            for (Broadcast broadcast : broadcasts) {
                org.atlasapi.media.entity.simple.Broadcast simpleBroadcast = simplify(broadcast, annotations);
                copyProperties(version, simpleBroadcast, item);
                simpleItem.addBroadcast(simpleBroadcast);
            }
        }
    }

    private Iterable<Broadcast> nextBroadcast(Iterable<Broadcast> broadcasts) {
        DateTime now = clock.now();
        DateTime earliest = null;
        Builder<Broadcast> filteredBroadcasts = ImmutableSet.builder();
        for (Broadcast broadcast : broadcasts) {
            DateTime transmissionTime = broadcast.getTransmissionTime();
            if (transmissionTime.isAfter(now) && (earliest == null || transmissionTime.isBefore(earliest))) {
                earliest = transmissionTime;
                filteredBroadcasts = ImmutableSet.<Broadcast>builder().add(broadcast);
            } else if (transmissionTime.isEqual(earliest)) {
                filteredBroadcasts.add(broadcast);
            }
        }
        return filteredBroadcasts.build();
    }

    private Iterable<Broadcast> firstBroadcasts(Iterable<Broadcast> broadcasts) {
        DateTime earliest = null;
        Builder<Broadcast> filteredBroadcasts = ImmutableSet.builder();
        for (Broadcast broadcast : broadcasts) {
            DateTime transmissionTime = broadcast.getTransmissionTime();
            if (earliest == null || transmissionTime.isBefore(earliest)) {
                earliest = transmissionTime;
                filteredBroadcasts = ImmutableSet.<Broadcast>builder().add(broadcast);
            } else if (transmissionTime.isEqual(earliest)) {
                filteredBroadcasts.add(broadcast);
            }
        }
        return filteredBroadcasts.build();
    }

    private Iterable<Broadcast> filterInactive(Iterable<Broadcast> broadcasts) {
        return Iterables.filter(broadcasts, new Predicate<Broadcast>() {

            @Override
            public boolean apply(Broadcast input) {
                return input.isActivelyPublished();
            }
        });
    }

    private org.atlasapi.media.entity.simple.Broadcast simplify(Broadcast broadcast, Set<Annotation> annotations) {
        org.atlasapi.media.entity.simple.Broadcast simpleModel = new org.atlasapi.media.entity.simple.Broadcast(broadcast.getBroadcastOn(), broadcast.getTransmissionTime(),
                broadcast.getTransmissionEndTime(), broadcast.getSourceId(), blackoutRestriction(broadcast));
        if (broadcast.getActualTransmissionTime() != null) {
            simpleModel.setActualTransmissionTime(broadcast.getActualTransmissionTime().toDate());
        }
        
        if (broadcast.getActualTransmissionEndTime() != null) {
            simpleModel.setActualTransmissionEndTime(broadcast.getActualTransmissionEndTime().toDate());
        }
        
        simpleModel.setRepeat(broadcast.getRepeat());
        simpleModel.setSubtitled(broadcast.getSubtitled());
        simpleModel.setSigned(broadcast.getSigned());
        simpleModel.setAudioDescribed(broadcast.getAudioDescribed());
        simpleModel.setHighDefinition(broadcast.getHighDefinition());
        simpleModel.setWidescreen(broadcast.getWidescreen());
        simpleModel.setSurround(broadcast.getSurround());
        simpleModel.setLive(broadcast.getLive());
        simpleModel.setPremiere(broadcast.getPremiere());
        simpleModel.setNewSeries(broadcast.getNewSeries());
        simpleModel.setNewEpisode(broadcast.getNewEpisode());
        simpleModel.setAliases(broadcast.getAliasUrls());
        Maybe<org.atlasapi.media.channel.Channel> channel = channelResolver.fromUri(broadcast.getBroadcastOn());
        if (channel.hasValue()) {
            simpleModel.setChannel(simplify(channel.requireValue(), annotations, 
                    Optional.<Image>absent(), Optional.<Set<Image>>absent()));
        } else {
            log.error("Could not resolve channel " + broadcast.getBroadcastOn());
        }

        return simpleModel;
    }
    
    private BlackoutRestriction blackoutRestriction(Broadcast broadcast) {
        if (broadcast.getBlackoutRestriction() != null
                && Boolean.TRUE.equals(broadcast.getBlackoutRestriction().getAll())) {
            return new BlackoutRestriction(true);
        } else {
            return null;
        }
    }
    
    private Channel simplify(org.atlasapi.media.channel.Channel channel, Set<Annotation> annotations, 
            Optional<Image> overrideImage, Optional<Set<Image>> overrideChannelImages) {
        Channel simpleChannel = new Channel();
        simpleChannel.setId(channelIdCodec.encode(BigInteger.valueOf(channel.getId())));
        if(annotations.contains(Annotation.CHANNEL_SUMMARY)) {
            simpleChannel.setTitle(channel.getTitle());
            
            if (overrideImage.isPresent()) {
                simpleChannel.setImage(overrideImage.get().getCanonicalUri());
            } else if (channel.getImage() != null) {
                simpleChannel.setImage(channel.getImage().getCanonicalUri());
            }
            
            simpleChannel.setHighDefinition(channel.getHighDefinition());
            simpleChannel.setRegional(channel.getRegional());
            if (channel.getTimeshift() != null) {
                simpleChannel.setTimeshift(channel.getTimeshift().getStandardSeconds());
            }
            Set<Image> channelImages = overrideChannelImages.or(channel.getImages());
            simpleChannel.setImages(Iterables.transform(
                channelImages, 
                new Function<Image, org.atlasapi.media.entity.simple.Image>() {
                    @Override
                    public org.atlasapi.media.entity.simple.Image apply(Image input) {
                        return imageSimplifier.simplify(input, ImmutableSet.<Annotation>of(), null);
                    }
                }
            ));
            simpleChannel.setParent(simplifyParentChannel(channel.getParent(), annotations));
        }
        return simpleChannel;
    }

    private Channel simplifyParentChannel(Long parent, Set<Annotation> annotations) {
        if (parent == null) {
            return null;
        }
        Maybe<org.atlasapi.media.channel.Channel> possibleChannel = channelResolver.fromId(parent);
        if (!possibleChannel.hasValue()) {
            return null;
        }
        org.atlasapi.media.channel.Channel channel = possibleChannel.requireValue();
        if (parent.equals(channel.getParent())) {
            // avoid an infinite loop. Something's wrong here, let's just give up.
            return null;
        }
        // Channel image overrides are to tide us over until we get images from source on
        // parent channels (a.k.a. stations).
        Long overrideChannel = channelImageOverrides.get(channel.getId());
        if (overrideChannel != null) {
            Maybe<org.atlasapi.media.channel.Channel> channelForImages = channelResolver.fromId(overrideChannel);
            if (channelForImages.hasValue()) {
                return simplify(channel, annotations,
                        Optional.of(channelForImages.requireValue().getImage()), 
                        Optional.of(channelForImages.requireValue().getImages()));
            }
        }
        return simplify(channel, annotations, Optional.<Image>absent(), Optional.<Set<Image>>absent());
    }

    private void copyProperties(Version version, org.atlasapi.media.entity.simple.Version simpleLocation, org.atlasapi.media.entity.Item item) {

        simpleLocation.setPublishedDuration(version.getPublishedDuration());
        simpleLocation.setDuration(durationFrom(item, version));
        simpleLocation.set3d(version.is3d());

        Restriction restriction = new Restriction();

        if (version.getRestriction() != null) {
            restriction.setRestricted(version.getRestriction().isRestricted());
            restriction.setMinimumAge(version.getRestriction().getMinimumAge());
            restriction.setMessage(version.getRestriction().getMessage());
        }

        simpleLocation.setRestriction(restriction);
    }

    // temporary fix: some versions are missing durations so
    // we fall back to the broadcast and location durations
    private Integer durationFrom(org.atlasapi.media.entity.Item item, Version version) {
        if (version.getDuration() != null && version.getDuration() > 0) {
            return version.getDuration();
        }
        Iterable<Broadcast> broadcasts = item.flattenBroadcasts();
        if (Iterables.isEmpty(broadcasts)) {
            return null;
        }
        return Ordering.natural().max(Iterables.transform(broadcasts, new Function<Broadcast, Integer>() {

            @Override
            public Integer apply(Broadcast input) {
                Integer duration = input.getBroadcastDuration();
                if (duration == null) {
                    return 0;
                }
                return duration;
            }
        }));
    }

    private void addTo(org.atlasapi.media.entity.simple.Item simpleItem, Version version, Encoding encoding, Item item, Set<Annotation> annotations, ApplicationConfiguration config) {
        DateTime now = new DateTime(DateTimeZones.UTC);
        for (Location location : encoding.getAvailableAt()) {
            if (!annotations.contains(Annotation.AVAILABLE_LOCATIONS) 
                    || location.getPolicy() == null 
                    || available(location.getPolicy(), now)) {
                addTo(simpleItem, version, encoding, location, item, annotations, config);
            }
        }
    }

    private boolean available(Policy policy, DateTime now) {
        return policy.getAvailabilityStart() == null
                || policy.getAvailabilityEnd() == null
                || policy.getAvailabilityStart().isBefore(now) && policy.getAvailabilityEnd().isAfter(now);
    }

    private void addTo(org.atlasapi.media.entity.simple.Item simpleItem, Version version, Encoding encoding, Location location, Item item, Set<Annotation> annotations, ApplicationConfiguration config) {

        org.atlasapi.media.entity.simple.Location simpleLocation = new org.atlasapi.media.entity.simple.Location();

        copyProperties(version, simpleLocation, item);
        copyProperties(encoding, simpleLocation);
        copyProperties(location, simpleLocation, annotations, config);

        simpleItem.addLocation(simpleLocation);
        
        if (annotations.contains(Annotation.V4_ALIASES)) {
            simpleLocation.setV4Aliases(ImmutableSet.copyOf(Iterables.transform(version.getAliases(), 
                    TO_SIMPLE_ALIAS)));
        }
    }

    private SeriesSummary seriesSummaryFromResolved(ParentRef seriesRef, Set<Annotation> annotations) {
        SeriesSummary baseSummary = new SeriesSummary();
        setIdAndUriFromParentRef(seriesRef, baseSummary);

        return annotations.contains(Annotation.SERIES_SUMMARY) ? containerSummaryResolver.summarizeSeries(seriesRef).or(baseSummary) : baseSummary;
    }

    private BrandSummary summaryFromResolved(ParentRef container, Set<Annotation> annotations) {
        BrandSummary baseSummary = new BrandSummary();
        setIdAndUriFromParentRef(container, baseSummary);

        return annotations.contains(Annotation.BRAND_SUMMARY) ? containerSummaryResolver.summarizeTopLevelContainer(container).or(baseSummary) : baseSummary;
    }
    
    private void setIdAndUriFromParentRef(ParentRef parentRef, Identified summary) {
        summary.setUri(parentRef.getUri());
        Long id = parentRef.getId();
        summary.setId(id != null ? idCodec.encode(BigInteger.valueOf(id)) : null);
    }

    private void copyProperties(Encoding encoding, org.atlasapi.media.entity.simple.Location simpleLocation) {

        simpleLocation.setAdvertisingDuration(encoding.getAdvertisingDuration());
        simpleLocation.setAudioBitRate(encoding.getAudioBitRate());
        simpleLocation.setAudioChannels(encoding.getAudioChannels());
        simpleLocation.setBitRate(encoding.getBitRate());
        simpleLocation.setContainsAdvertising(encoding.getContainsAdvertising());
        if (encoding.getDataContainerFormat() != null) {
            simpleLocation.setDataContainerFormat(encoding.getDataContainerFormat().toString());
        }
        simpleLocation.setDataSize(encoding.getDataSize());
        simpleLocation.setDistributor(encoding.getDistributor());
        simpleLocation.setHasDOG(encoding.getHasDOG());
        simpleLocation.setSource(encoding.getSource());
        simpleLocation.setVideoAspectRatio(encoding.getVideoAspectRatio());
        simpleLocation.setVideoBitRate(encoding.getVideoBitRate());

        if (encoding.getVideoCoding() != null) {
            simpleLocation.setVideoCoding(encoding.getVideoCoding().toString());
        }
        if (encoding.getAudioCoding() != null) {
            simpleLocation.setAudioCoding(encoding.getAudioCoding().toString());
        }

        simpleLocation.setAudioDescribed(encoding.getAudioDescribed());
        simpleLocation.setSigned(encoding.getSigned());
        simpleLocation.setVideoFrameRate(encoding.getVideoFrameRate());
        simpleLocation.setVideoHorizontalSize(encoding.getVideoHorizontalSize());
        simpleLocation.setVideoProgressiveScan(encoding.getVideoProgressiveScan());
        simpleLocation.setVideoVerticalSize(encoding.getVideoVerticalSize());
    }

    private void copyProperties(Location location, org.atlasapi.media.entity.simple.Location simpleLocation, Set<Annotation> annotations, ApplicationConfiguration config) {
        Policy policy = location.getPolicy();
        if (policy != null) {
            if (policy.getActualAvailabilityStart() != null) {
                simpleLocation.setActualAvailabilityStart(policy.getActualAvailabilityStart().toDate());
            }
            if (policy.getAvailabilityStart() != null) {
                simpleLocation.setAvailabilityStart(policy.getAvailabilityStart().toDate());
            }
            if (policy.getAvailabilityEnd() != null) {
                simpleLocation.setAvailabilityEnd(policy.getAvailabilityEnd().toDate());
            }
            if (policy.getDrmPlayableFrom() != null) {
                simpleLocation.setDrmPlayableFrom(policy.getDrmPlayableFrom().toDate());
            }
            if (policy.getAvailableCountries() != null) {
                simpleLocation.setAvailableCountries(Countries.toCodes(policy.getAvailableCountries()));
            }
            if (policy.getRevenueContract() != null) {
                simpleLocation.setRevenueContract(policy.getRevenueContract().key());
            }
            if (policy.getPrice() != null) {
                simpleLocation.setPrice(policy.getPrice().getAmount());
                simpleLocation.setCurrency(policy.getPrice().getCurrency().getCurrencyCode());
            }
            if (policy.getPlatform() != null) {
                simpleLocation.setPlatform(policy.getPlatform().key());
            }
            if (policy.getNetwork() != null) {
                simpleLocation.setNetwork(policy.getNetwork().key());
            }
            if (policy.getService() != null) {
                Optional<Service> service = serviceResolver.serviceFor(policy.getService());
                if (service.isPresent()) {
                    simpleLocation.setService(serviceModelSimplifier.simplify(service.get(), 
                            annotations, config));
                }
            }
            
            if (policy.getPlayer() != null) {
                Optional<Player> player = playerResolver.playerFor(policy.getPlayer());
                if (player.isPresent()) {
                    simpleLocation.setPlayer(playerModelSimplifier.simplify(player.get(), 
                            annotations, config));
                }
            }
        }

        simpleLocation.setTransportIsLive(location.getTransportIsLive());
        if (location.getTransportType() != null) {
            simpleLocation.setTransportType(location.getTransportType().toString());
        }
        if (location.getTransportSubType() != null) {
            simpleLocation.setTransportSubType(location.getTransportSubType().toString());
        }
        simpleLocation.setUri(location.getUri());
        simpleLocation.setEmbedCode(location.getEmbedCode());
        simpleLocation.setEmbedId(location.getEmbedId());
        simpleLocation.setAvailable(location.getAvailable());

    }
}
