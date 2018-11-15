package org.atlasapi.input;

import java.util.stream.Collectors;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.repackaged.com.google.common.base.Strings;

public class ChannelGroupTransformer implements
        ModelTransformer<org.atlasapi.media.entity.simple.ChannelGroup, ChannelGroup> {

    private static final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();

    @Override
    public ChannelGroup transform(org.atlasapi.media.entity.simple.ChannelGroup simple) {
        Platform complex = new Platform();

        //set the fields we agreed on with BT. Check BT Channel Groups in Google Docs for more info
        if (!Strings.isNullOrEmpty(simple.getId())) {
            complex.setId(idCodec.decode(simple.getId()).longValue());
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
