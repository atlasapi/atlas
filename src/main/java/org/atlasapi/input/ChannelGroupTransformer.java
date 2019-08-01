package org.atlasapi.input;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import org.joda.time.LocalDate;

public class ChannelGroupTransformer implements
        ModelTransformer<org.atlasapi.media.entity.simple.ChannelGroup, ChannelGroup> {

    private static final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();

    @Override
    public ChannelGroup transform(org.atlasapi.media.entity.simple.ChannelGroup simple) {
        Platform complex = new Platform();

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

    private void setChannelNumbers(
            org.atlasapi.media.entity.simple.ChannelGroup simple,
            Platform complex,
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
