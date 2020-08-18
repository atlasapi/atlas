package org.atlasapi.input;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.LocalDate;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.gdata.util.common.base.Preconditions.checkArgument;
import static com.google.gdata.util.common.base.Preconditions.checkNotNull;

public class ChannelModelTransformer implements ModelTransformer<Channel, org.atlasapi.media.channel.Channel> {

    private final NumberToShortStringCodec v4Codec;
    private final ImageModelTranslator imageTranslator;

    private ChannelModelTransformer(
            NumberToShortStringCodec v4Codec,
            ImageModelTranslator imageTranslator
    ) {
        this.v4Codec = checkNotNull(v4Codec);
        this.imageTranslator = checkNotNull(imageTranslator);
    }

    public static ChannelModelTransformer create(
            NumberToShortStringCodec v4Codec,
            ImageModelTranslator imageTranslator
    ) {
        return new ChannelModelTransformer(v4Codec, imageTranslator);
    }

    @Override
    public org.atlasapi.media.channel.Channel transform(Channel simple) {
        checkArgument(simple.getUri() != null, "Channel uri should be provided");

        org.atlasapi.media.channel.Channel.Builder complex = org.atlasapi.media.channel.Channel.builder();

        complex.withUri(simple.getUri());
        complex.withKey(simple.getUri());
        complex.withHighDefinition(simple.getHighDefinition());
        complex.withTimeshifted(simple.isTimeshifted());
        complex.withRegional(simple.getRegional());
        complex.withRegion(simple.getRegion());
        complex.withAdult(simple.getAdult());
        complex.withTitle(simple.getTitle());
        complex.withChannelType(ChannelType.valueOf(simple.getChannelType().toUpperCase()));

        if (simple.getMediaType() != null) {
            Optional<MediaType> mediaType = MediaType.fromKey(simple.getMediaType());
            if (mediaType.isPresent()) {
                complex.withMediaType(mediaType.get());
            } else {
                throw new IllegalArgumentException("Couldn't get media type from simple model, "
                        + "media type either not AUDIO or VIDEO.");
            }
        }

        if (simple.getPublisherDetails() != null) {
            Maybe<Publisher> publisher = Publisher.fromKey(simple.getPublisherDetails().getKey());
            if (!publisher.isNothing()) {
                complex.withSource(publisher.requireValue());
            } else {
                throw new IllegalArgumentException("Publisher is not present while trying to set "
                        + "source.");
            }
        }

        if (simple.getBroadcaster() != null) {
            Maybe<Publisher> publisher = Publisher.fromKey(simple.getBroadcaster().getKey());
            if (!publisher.isNothing()) {
                complex.withBroadcaster(publisher.requireValue());
            } else {
                throw new IllegalArgumentException("Couldn't extract publisher from simple "
                        + "model broadcaster field.");
            }
        }

        if (!simple.getTargetRegions().isEmpty()) {
            complex.withTargetRegions(simple.getTargetRegions());
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

        if (simple.getAvailableFrom() != null && !simple.getAvailableFrom().isEmpty()) {
            List<Publisher> publisherDetails = Lists.newArrayList();

            simple.getAvailableFrom()
                    .stream()
                    .forEach(publisher ->
                            publisherDetails.add(translatePublisherDetails(publisher))
                    );

            complex.withAvailableFrom(publisherDetails);
        }

        if (simple.getParent() != null) {
            org.atlasapi.media.channel.Channel.Builder parent
                    = org.atlasapi.media.channel.Channel.builder();
            parent.withUri(simple.getParent().getUri());
            complex.withParent(parent.build());
        }

        if (simple.getVariations() != null && !simple.getVariations().isEmpty()) {
            List<org.atlasapi.media.channel.Channel> variations = Lists.newArrayList();

            simple.getVariations()
                    .stream()
                    .forEach(variation ->
                            variations.add(transform(variation))
                    );

            complex.withVariations(variations);
        }

        simple.getRelatedLinks()
                .stream()
                .forEach(relatedLink -> complex.withRelatedLink(
                        translateRelatedLink(relatedLink)
                ));

        if (simple.getChannelGroups() != null && !simple.getChannelGroups().isEmpty()) {
            List<ChannelNumbering> channelNumberings = Lists.newArrayList();

            simple.getChannelGroups()
                    .stream()
                    .forEach(channelNumbering ->
                            channelNumberings.add(translateChannelNumbering(channelNumbering))
                    );

            complex.withChannelNumbers(channelNumberings);
        }

        if (simple.getV4Aliases() != null && !simple.getV4Aliases().isEmpty()) {
            Set<Alias> aliases = Sets.newHashSet();

            simple.getV4Aliases()
                    .stream()
                    .forEach(alias -> aliases.add(translateAlias(alias)));

            complex.withAliases(aliases);
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
        } else {
            throw new IllegalArgumentException("Publisher is not present while extracting "
                    + "publisher details from the simple model.");
        }
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

    private Alias translateAlias(org.atlasapi.media.entity.simple.Alias alias) {
        return new Alias(alias.getNamespace(), alias.getValue());
    }
}
