package org.atlasapi.input;

import java.util.List;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.intl.Countries;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.util.Lists;
import org.joda.time.LocalDate;

public class ChannelGroupTransformer implements
        ModelTransformer<org.atlasapi.media.entity.simple.ChannelGroup, ChannelGroup> {

    @Override
    public ChannelGroup transform(org.atlasapi.media.entity.simple.ChannelGroup simple) {
        Platform complex = new Platform();

        complex.setPublisher(getPublisher(simple.getPublisherDetails()));
        complex.addTitle(simple.getTitle());
        complex.setAvailableCountries(Countries.fromCodes(simple.getAvailableCountries()));

        List<ChannelNumbering> channelNumberingList = Lists.newArrayList();
        simple.getChannels().forEach(channelNumbering -> channelNumberingList.add(
                ChannelNumbering.builder()
                        .withChannel(Long.valueOf(channelNumbering.getChannel().getId()))
                        .withChannelGroup(Long.valueOf(channelNumbering.getChannelGroup().getId()))
                        .withChannelNumber(channelNumbering.getChannelNumber())
                        .withStartDate(LocalDate.fromDateFields(channelNumbering.getStartDate()))
                        .withEndDate(LocalDate.fromDateFields(channelNumbering.getEndDate()))
                        .build()
        ));
        complex.setChannelNumberings(channelNumberingList);

        return complex;
    }

    private Publisher getPublisher(PublisherDetails publisherDetails) {
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
