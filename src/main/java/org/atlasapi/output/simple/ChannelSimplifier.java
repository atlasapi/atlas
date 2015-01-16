package org.atlasapi.output.simple;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.simple.ChannelGroupSummary;
import org.atlasapi.media.entity.simple.HistoricalChannelEntry;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class ChannelSimplifier {

    private final NumberToShortStringCodec idCodec;
    private final ChannelResolver channelResolver;
    private final PublisherSimplifier publisherSimplifier;
    private final NumberToShortStringCodec v4Codec;
    private final ImageSimplifier imageSimplifier;
    private final ChannelGroupSummarySimplifier channelGroupAliasSimplifier;
    private final ChannelGroupResolver channelGroupResolver;
    
    public ChannelSimplifier(NumberToShortStringCodec idCodec, NumberToShortStringCodec v4Codec, 
            ChannelResolver channelResolver, PublisherSimplifier publisherSimplifier, 
            ImageSimplifier imageSimplifier, ChannelGroupSummarySimplifier channelGroupSummarySimplifier, 
            ChannelGroupResolver channelGroupResolver) {
        this.idCodec = checkNotNull(idCodec);
        this.v4Codec = checkNotNull(v4Codec);
        this.channelResolver = checkNotNull(channelResolver);
        this.publisherSimplifier = checkNotNull(publisherSimplifier);
        this.imageSimplifier = checkNotNull(imageSimplifier);
        this.channelGroupAliasSimplifier = checkNotNull(channelGroupSummarySimplifier);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
    }

    public org.atlasapi.media.entity.simple.Channel simplify(Channel input, final boolean showHistory, 
            boolean showParent, final boolean showVariations, final boolean showGroupSummary,
            final ApplicationConfiguration config) {
        
        org.atlasapi.media.entity.simple.Channel simple = new org.atlasapi.media.entity.simple.Channel();
        
        simple.setType("channel");
        simple.setUri(input.getCanonicalUri());
        if (input.getId() != null) {
            simple.setId(idCodec.encode(BigInteger.valueOf(input.getId())));
        }

        simple.setAliases(Sets.union(input.getAliasUrls(), ImmutableSet.of(createV4AliasUrl(input))));
        simple.setHighDefinition(input.getHighDefinition());
        simple.setRegional(input.getRegional());
        simple.setAdult(input.getAdult());
        if (input.getTimeshift() != null) {
            simple.setTimeshift(input.getTimeshift().getStandardSeconds());
        }
        simple.setTitle(input.getTitle());
        Image image = input.getImage();
        if (image != null) {
            simple.setImage(image.getCanonicalUri());
        }
        simple.setImages(Iterables.transform(
            input.getImages(), 
            new Function<Image, org.atlasapi.media.entity.simple.Image>() {
                @Override
                public org.atlasapi.media.entity.simple.Image apply(Image input) {
                    return imageSimplifier.simplify(input, ImmutableSet.<Annotation>of(), null);
                }
            }
        ));
        simple.setMediaType(input.getMediaType() != null ? input.getMediaType().toString().toLowerCase() : null);
        simple.setRelatedLinks(simplifyRelatedLinks(input.getRelatedLinks()));
        simple.setStartDate(input.getStartDate());            
        simple.setRelatedLinks(simplifyRelatedLinks(input.getRelatedLinks()));
        simple.setEndDate(input.getEndDate());    
        simple.setGenres(input.getGenres());
        
        simple.setPublisherDetails(publisherSimplifier.simplify(input.getSource()));
        simple.setBroadcaster(publisherSimplifier.simplify(input.getBroadcaster()));
        simple.setAvailableFrom(Iterables.transform(input.getAvailableFrom(), new Function<Publisher, PublisherDetails>() {
            @Override
            public PublisherDetails apply(Publisher input) {
                return publisherSimplifier.simplify(input);
            }
        }));
        
        if (input.getParent() != null) {
            Maybe<Channel> channel = channelResolver.fromId(input.getParent());
            if (!channel.hasValue()) {
                throw new RuntimeException("Could not resolve channel with id " +  input.getParent());
            }
            if (showParent) {
                simple.setParent(simplify(channel.requireValue(), showHistory, false, false, showGroupSummary, config));
            } else {
                simple.setParent(toSubChannel(channel.requireValue()));
            }
        }
        if (input.getVariations() != null && !input.getVariations().isEmpty()) {
            simple.setVariations(Iterables.transform(
                channelResolver.forIds(input.getVariations()), 
                new Function<Channel, org.atlasapi.media.entity.simple.Channel>() {
                    @Override
                    public org.atlasapi.media.entity.simple.Channel apply(Channel input) {
                        if (showVariations) {
                            return simplify(input, showHistory, false, false, showGroupSummary, config);
                        } else {
                            return toSubChannel(input);
                        }
                    }
                }
            ));
        }
        
        if (showHistory) {            
            simple.setHistory(calculateChannelHistory(input));
        }
        
        
        if (showGroupSummary) {
            Iterable<ChannelGroup> groups = channelGroupResolver.channelGroupsFor(Iterables.transform(input.getChannelNumbers(), ChannelNumbering.TO_CHANNEL_GROUP));
            simple.setGroups(
                    FluentIterable
                            .from(groups)
                            .filter(new Predicate<ChannelGroup>() {

                                @Override
                                public boolean apply(ChannelGroup input) {
                                    return config.isEnabled(input.getPublisher());
                                }})
                                
                            .transform(TO_CHANNEL_GROUP_ALIAS));
        }

        return simple;
    }
    
    private final Function<ChannelGroup, ChannelGroupSummary> TO_CHANNEL_GROUP_ALIAS = new Function<ChannelGroup, ChannelGroupSummary>() {

        @Override
        public ChannelGroupSummary apply(ChannelGroup numbering) {
            return channelGroupAliasSimplifier.simplify(numbering);
        }
        
    };
    
    public Iterable<org.atlasapi.media.entity.simple.RelatedLink> simplifyRelatedLinks(Iterable<RelatedLink> relatedLinks) {
        return Iterables.transform(relatedLinks, new Function<RelatedLink, org.atlasapi.media.entity.simple.RelatedLink>() {

            @Override
            public org.atlasapi.media.entity.simple.RelatedLink apply(RelatedLink relatedLink) {
                org.atlasapi.media.entity.simple.RelatedLink simpleLink = new org.atlasapi.media.entity.simple.RelatedLink();

                simpleLink.setUrl(relatedLink.getUrl());
                simpleLink.setType(relatedLink.getType().toString().toLowerCase());
                simpleLink.setSourceId(relatedLink.getSourceId());
                simpleLink.setShortName(relatedLink.getShortName());
                simpleLink.setTitle(relatedLink.getTitle());
                simpleLink.setDescription(relatedLink.getDescription());
                simpleLink.setImage(relatedLink.getImage());
                simpleLink.setThumbnail(relatedLink.getThumbnail());

                return simpleLink;
            }
        });
    }
    
    private Set<HistoricalChannelEntry> calculateChannelHistory(Channel input) {
        Builder<HistoricalChannelEntry> entries = ImmutableSet.<HistoricalChannelEntry>builder();
        for (TemporalField<String> title : input.getAllTitles()) {
            if (title.getStartDate() == null) {
                continue;
            }
            HistoricalChannelEntry entry = new HistoricalChannelEntry(title.getStartDate());
            entry.setTitle(title.getValue());
            Iterable<Image> primaryImages = Iterables.filter(
                input.getImagesForDate(title.getStartDate()), 
                Channel.IS_PRIMARY_IMAGE
            );
            if (!Iterables.isEmpty(primaryImages)) {
                entry.setImage(Iterables.getOnlyElement(primaryImages).getCanonicalUri());
            }
            entry.setImages(Iterables.transform(
                input.getImagesForDate(title.getStartDate()), 
                new Function<Image, org.atlasapi.media.entity.simple.Image>() {
                    @Override
                    public org.atlasapi.media.entity.simple.Image apply(Image input) {
                        return imageSimplifier.simplify(input, ImmutableSet.<Annotation>of(), null);
                    }
                }
            ));
            entries.add(entry);
        }
        for (TemporalField<Image> image : input.getAllImages()) {
            if (image.getStartDate() == null) {
                continue;
            }
            HistoricalChannelEntry entry = new HistoricalChannelEntry(image.getStartDate());
            entry.setTitle(input.getTitleForDate(image.getStartDate()));
            entry.setImage(image.getValue().getCanonicalUri());
            entry.setImages(Iterables.transform(
                input.getImagesForDate(image.getStartDate()), 
                new Function<Image, org.atlasapi.media.entity.simple.Image>() {
                    @Override
                    public org.atlasapi.media.entity.simple.Image apply(Image input) {
                        return imageSimplifier.simplify(input, ImmutableSet.<Annotation>of(), null);
                    }
                }
            ));
            entries.add(entry);
        }
        return entries.build();
    }

    private org.atlasapi.media.entity.simple.Channel toSubChannel(Channel input) {
        org.atlasapi.media.entity.simple.Channel simple = new org.atlasapi.media.entity.simple.Channel();
        simple.setId(idCodec.encode(BigInteger.valueOf(input.getId())));
        simple.setTitle(input.getTitle());
        return simple;
    }
    
    private String createV4AliasUrl(Channel input) {
        return String.format("http://atlas.metabroadcast.com/4.0/channels/%s", v4Codec.encode(BigInteger.valueOf(input.getId())));
    }
}
