package org.atlasapi.input;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.joda.time.LocalDate;

public class ChannelGroupTransformer implements
        ModelTransformer<org.atlasapi.media.entity.simple.ChannelGroup, ChannelGroup> {

    private static final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
    public static final String REGION = "region";
    public static final String PLATFORM = "platform";

    @Override
    public ChannelGroup transform(org.atlasapi.media.entity.simple.ChannelGroup simple) {
        ChannelGroup complex;
        if(REGION.equals(simple.getType())) {
            Region region = new Region();
            if (simple.getPlatform() != null && PLATFORM.equals(simple.getPlatform().getType())) {
                region.setPlatform(idCodec.decode(simple.getPlatform().getId()).longValue());
            }
            complex = region;
        }
        else {
            Platform platform = new Platform();
            if (simple.getRegions() != null && !simple.getRegions().isEmpty()) {
                platform.setRegionIds(transformInnerRegions(simple));
            }
            complex = platform;
        }

        //set the fields we agreed on with BT. Check BT Channel Groups in Google Docs for more info
        if (!Strings.isNullOrEmpty(simple.getId())) {
            long channelGroupId = idCodec.decode(simple.getId()).longValue();
            complex.setId(channelGroupId);
            setChannelNumbers(simple, complex, channelGroupId);
        }
        complex.setCanonicalUri(simple.getUri());
        complex.addTitle(simple.getTitle());
        complex.setPublisher(extractPublisher(simple.getPublisherDetails()));
        complex.setAliases(simple.getV4Aliases()
                .stream()
                .map(alias -> new Alias(alias.getNamespace(), alias.getValue()))
                .collect(Collectors.toSet())
        );

        return complex;
    }

    private List<Long> transformInnerRegions(org.atlasapi.media.entity.simple.ChannelGroup simple) {
        Set<org.atlasapi.media.entity.simple.ChannelGroup> simpleRegions = simple.getRegions();
        List<Long> regions = Lists.newArrayListWithCapacity(simple.getRegions().size());
        for (org.atlasapi.media.entity.simple.ChannelGroup simpleRegion : simpleRegions) {
            if (REGION.equals(simpleRegion.getType())) {
                regions.add(idCodec.decode(simple.getId()).longValue());
            }
        }
        return regions;
    }

    private void setChannelNumbers(
            org.atlasapi.media.entity.simple.ChannelGroup simple,
            ChannelGroup complex,
            long channelGroupId
    ) {
        Set<ChannelNumbering> channelNumberings = simple.getChannels()
                .stream()
                .map(channelNumbering -> ChannelNumbering.builder()
                        .withChannel(idCodec.decode(channelNumbering.getChannel().getId()).longValue())
                        .withChannelGroup(channelGroupId)
                        .withChannelNumber(channelNumbering.getChannelNumber())
                        .withStartDate(Objects.isNull(channelNumbering.getStartDate())
                                       ? null
                                       : LocalDate.fromDateFields(channelNumbering.getStartDate()))
                        .withEndDate(Objects.isNull(channelNumbering.getEndDate())
                                     ? null
                                     : LocalDate.fromDateFields(channelNumbering.getEndDate()))
                        .build()
                )
                .collect(Collectors.toSet());
        complex.setChannelNumberings(channelNumberings);
    }

    private Publisher extractPublisher(PublisherDetails publisherDetails) {
        if (Strings.isNullOrEmpty(publisherDetails.getKey())) {
            throw new IllegalArgumentException("Missing publisher key.");
        }

        Maybe<Publisher> possiblePublisher = Publisher.fromKey(publisherDetails.getKey());
        if (possiblePublisher.isNothing()) {
            throw new IllegalArgumentException(String.format(
                    "Unknown publisher with key %s",
                    publisherDetails.getKey()
            ));
        }

        return possiblePublisher.requireValue();
    }
}
