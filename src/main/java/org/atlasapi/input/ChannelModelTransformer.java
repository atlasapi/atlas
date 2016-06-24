package org.atlasapi.input;

import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.PublisherDetails;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.LocalDate;

public class ChannelModelTransformer implements ModelTransformer<Channel, org.atlasapi.media.channel.Channel> {

    private final NumberToShortStringCodec v4Codec;
    private final ImageModelTranslator imageTranslator;

    public ChannelModelTransformer(
            NumberToShortStringCodec v4Codec,
            ImageModelTranslator imageTranslator
    ) {
        this.v4Codec = v4Codec;
        this.imageTranslator = imageTranslator;
    }

    @Override
    public org.atlasapi.media.channel.Channel transform(Channel simple) {
        org.atlasapi.media.channel.Channel.Builder complex = org.atlasapi.media.channel.Channel.builder();

        complex.withUri(simple.getUri());
        complex.withHighDefinition(simple.getHighDefinition());
        complex.withRegional(simple.getRegional());
        complex.withAdult(simple.getAdult());
        complex.withTitle(simple.getTitle());

        Set<Alias> aliases = simple.getV4Aliases().stream().map(simpleAlias -> new Alias(
                simpleAlias.getNamespace(),
                simpleAlias.getValue()
        )).collect(Collectors.toSet());
        complex.withAliases(aliases);

        if (simple.getMediaType() != null) {
            Optional<MediaType> mediaType = MediaType.fromKey(simple.getMediaType().toUpperCase());
            if (mediaType.isPresent()) {
                complex.withMediaType(mediaType.get());
            }
        }

        if (simple.getStartDate() != null) {
            LocalDate startDate = new LocalDate(simple.getStartDate());
            complex.withStartDate(startDate);
        }

        if (simple.getEndDate() != null) {
            LocalDate endDate = new LocalDate(simple.getEndDate());
            complex.withEndDate(endDate);
        }

        if (simple.getTimeshift() != null) {
            complex.withTimeshift(Duration.standardSeconds(simple.getTimeshift()));
        }

        if (simple.getImage() != null) {
            complex.withImage(Image.builder(simple.getImage()).build());
        }

        if (simple.getAdvertisedFrom() != null) {
            complex.withAdvertiseFrom(new DateTime(simple.getAdvertisedFrom()));
        }

        if (simple.getGenres() != null && !simple.getGenres().isEmpty()) {
            complex.withGenres(simple.getGenres());
        }

        if (simple.getPublisherDetails() != null) {
            Maybe<Publisher> publisher = Publisher.fromKey(simple.getPublisherDetails().getKey());
            if (!publisher.isNothing()) {
                complex.withSource(publisher.requireValue());
            }
        }

        if (simple.getBroadcaster() != null) {
            Maybe<Publisher> publisher = Publisher.fromKey(simple.getBroadcaster().getKey());
            if (!publisher.isNothing()) {
                complex.withBroadcaster(publisher.requireValue());
            }
        }

        if (simple.getImages() != null && !simple.getImages().isEmpty()) {
            complex.withImages(Iterables.transform(
                    simple.getImages(),
                    input -> imageTranslator.transform(input)
            ));
        }

        if (simple.getAvailableFrom() != null && !simple.getAvailableFrom().isEmpty()) {
            complex.withAvailableFrom(Iterables.transform(
                    simple.getAvailableFrom(),
                    input -> translatePublisherDetails(input)
            ));
        }

        if (simple.getParent() != null) {
            org.atlasapi.media.channel.Channel.Builder parent
                    = org.atlasapi.media.channel.Channel.builder();
            parent.withUri(simple.getParent().getUri());
            complex.withParent(parent.build());
        }

        if (simple.getVariations() != null && !simple.getVariations().isEmpty()) {
            complex.withVariations(Iterables.transform(
                    simple.getVariations(),
                    channel -> transform(channel)
            ));
        }

        simple.getRelatedLinks().stream().forEach(relatedLink -> {
            complex.withRelatedLink(translateRelatedLink(relatedLink));
        });

        if (simple.getChannelGroups() != null && !simple.getChannelGroups().isEmpty()) {
            complex.withChannelNumbers(Iterables.transform(
                    simple.getChannelGroups(),
                    channelNumbering -> translateChannelNumbering(channelNumbering)
            ));
        }

        return complex.build();
    }

    private RelatedLink translateRelatedLink(org.atlasapi.media.entity.simple.RelatedLink simple) {
        RelatedLink.Builder complex = new RelatedLink.Builder(
                RelatedLink.LinkType.valueOf(simple.getType().toUpperCase()),
                simple.getUrl()
        );

        complex.withTitle(simple.getTitle());
        complex.withDescription(simple.getDescription());
        complex.withImage(simple.getImage());
        complex.withShortName(simple.getShortName());
        complex.withSourceId(simple.getSourceId());
        complex.withThumbnail(simple.getThumbnail());

        return complex.build();
    }

    private Publisher translatePublisherDetails(PublisherDetails simple) {
        Maybe<Publisher> complex = Publisher.fromKey(simple.getKey());
        if (!complex.isNothing()) {
            return complex.requireValue();
        }
        return null;
    }

    private ChannelNumbering translateChannelNumbering(
            org.atlasapi.media.entity.simple.ChannelNumbering simple) {
        ChannelNumbering.Builder complex = ChannelNumbering.builder();

        if (simple.getChannel() != null) {
            complex.withChannel(transform(simple.getChannel()));
        }

        if (simple.getChannelNumber() != null) {
            complex.withChannelNumber(simple.getChannelNumber());
        }

        if (simple.getStartDate() != null) {
            complex.withStartDate(new LocalDate(simple.getStartDate()));
        }

        if (simple.getEndDate() != null) {
            complex.withEndDate(new LocalDate(simple.getEndDate()));
        }

        if (simple.getChannelGroup() != null) {
            BigInteger id = v4Codec.decode(simple.getChannelGroup().getId());
            complex.withChannelGroup(id.longValue());
        }

        return complex.build();
    }
}
