package org.atlasapi.input;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.intl.Countries;

import com.google.api.client.repackaged.com.google.common.base.Strings;

public class ChannelGroupTransformer implements
        ModelTransformer<org.atlasapi.media.entity.simple.ChannelGroup, ChannelGroup> {

    private SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();

    @Override
    public ChannelGroup transform(org.atlasapi.media.entity.simple.ChannelGroup simple) {
        Platform complex = new Platform();

        if (Strings.isNullOrEmpty(simple.getId())) {
            complex.setId(idCodec.decode(simple.getId()).longValue());
        }

        complex.setPublisher(getPublisher(simple.getPublisherDetails()));
        complex.addTitle(simple.getTitle());
        complex.setAvailableCountries(Countries.fromCodes(simple.getAvailableCountries()));

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
