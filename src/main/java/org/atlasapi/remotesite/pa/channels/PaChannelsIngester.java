package org.atlasapi.remotesite.pa.channels;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.remotesite.pa.PaChannelMap;
import org.atlasapi.remotesite.pa.channels.bindings.Genres;
import org.atlasapi.remotesite.pa.channels.bindings.Logo;
import org.atlasapi.remotesite.pa.channels.bindings.Name;
import org.atlasapi.remotesite.pa.channels.bindings.ProviderChannelId;
import org.atlasapi.remotesite.pa.channels.bindings.ServiceProvider;
import org.atlasapi.remotesite.pa.channels.bindings.Station;
import org.atlasapi.remotesite.pa.channels.bindings.Url;
import org.atlasapi.remotesite.pa.channels.bindings.Variation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.remotesite.pa.channels.PaChannelGroupsIngester.getServiceProvider;

public class PaChannelsIngester {

    public static final String BT_SERVICE_ID_ALIAS_PREFIX = "http://bt.youview.com/service/";
    public static final String YOUVIEW_SERVICE_ID_ALIAS_PREFIX = "http://youview.com/service/";

    @VisibleForTesting
    static final String CHANNEL_URI_PREFIX =
            "http://ref.atlasapi.org/channels/pressassociation.com/";
    @VisibleForTesting
    static final String STATION_URI_PREFIX =
            "http://ref.atlasapi.org/channels/pressassociation.com/stations/";

    private static final Logger log = LoggerFactory.getLogger(PaChannelsIngester.class);
    private static final String GENRE_ADULT = "Adult";
    private static final String SIMULCAST_LINK_TYPE = "Web_Simulcast";

    private static final String REGIONAL_VARIATION = "regional";

    private static final String IMAGE_PREFIX =
            "http://images.atlas.metabroadcast.com/pressassociation.com/channels/";
    private static final String STATION_ALIAS_PREFIX = "http://pressassociation.com/stations/";
    private static final String GENRE_URI_PREFIX = "http://pressassociation.com/genres/";
    private static final String FORMAT_HD = "HD";

    private static final Map<String, String> YOUVIEW_SERVICE_PROVIDERS_TO_ALIAS_PREFIX =
            ImmutableMap.of(
                    "YouView", YOUVIEW_SERVICE_ID_ALIAS_PREFIX,
                    "BT TV", BT_SERVICE_ID_ALIAS_PREFIX
            );

    public static final Iterable<String> YOUVIEW_SERVICE_ID_ALIAS_PREFIXES =
            YOUVIEW_SERVICE_PROVIDERS_TO_ALIAS_PREFIX.values();

    private static final Map<String, MediaType> MEDIA_TYPE_MAPPING =
            ImmutableMap.<String, MediaType>builder()
                    .put("TV", MediaType.VIDEO)
                    .put("Radio", MediaType.AUDIO)
                    .build();

    private static final Map<String, ImageTheme> IMAGE_THEME_MAPPING =
            ImmutableMap.<String, ImageTheme>builder()
                    .put("Normal", ImageTheme.LIGHT_OPAQUE)
                    .put("Transparent Dark", ImageTheme.DARK_TRANSPARENT)
                    .put("Transparent Light", ImageTheme.LIGHT_TRANSPARENT)
                    .build();

    private static final Predicate<Variation> IS_REGIONAL =
            input -> REGIONAL_VARIATION.equals(input.getType());
    private static final String PA_CHANNEL_ID_NAMESPACE = "pa:channel:id";

    private final DateTimeFormatter formatter = ISODateTimeFormat.date();

    public ChannelTree processStation(Station station, List<ServiceProvider> serviceProviders) {
        try {
            if (!station.getChannels().getChannel().isEmpty()) {
                Optional<Boolean> isAdult = Optional.empty();
                Set<String> genres = Sets.newHashSet();
                if (station.getGenres() != null) {
                    isAdult = isAdult(station.getGenres());
                    genres = parseGenres(station.getGenres());
                }
                if (station.getChannels().getChannel().size() == 1) {
                    return new ChannelTree(
                            null,
                            ImmutableList.of(
                                    processStandaloneChannel(
                                            station.getChannels().getChannel().get(0),
                                            serviceProviders,
                                            genres,
                                            isAdult.orElse(null)
                                    )
                            )
                    );
                } else {
                    Channel parent = processParentChannel(
                            station,
                            station.getChannels().getChannel().get(0),
                            genres,
                            isAdult.orElse(null)
                    );
                    List<Channel> children = processChildChannels(
                            station.getChannels().getChannel(),
                            serviceProviders,
                            genres,
                            isAdult.orElse(null)
                    );
                    return new ChannelTree(parent, children);
                }
            } else {
                log.error("Station with id {} has no channels", station.getId());
            }
        } catch (Exception e) {
            log.error("Exception thrown while processing station with id {}", station.getId(), e);
        }
        return new ChannelTree(null, ImmutableList.of());
    }

    private List<Channel> processChildChannels(
            List<org.atlasapi.remotesite.pa.channels.bindings.Channel> channels,
            List<ServiceProvider> serviceProviders,
            Set<String> genres,
            Boolean isAdult
    ) {
        Builder<Channel> children = ImmutableList.builder();
        for (org.atlasapi.remotesite.pa.channels.bindings.Channel paChannel : channels) {
            children.add(processStandaloneChannel(paChannel, serviceProviders, genres, isAdult));
        }
        return children.build();
    }

    private String generateChannelKey(String id) {
        return "pa-channel-" + id;
    }

    private String generateStationKey(String id) {
        return "pa-station-" + id;
    }

    private Optional<Boolean> isAdult(Genres genres) {
        String genre = genres.getGenre();
        if (genre.equals(GENRE_ADULT)) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    // Genre uris are simply a standard prefix, followed by the genre value, lowercased,
    // with spaces replaced with underscores
    private Set<String> parseGenres(Genres genres) {
        String genre = genres.getGenre();
        return Sets.newHashSet(GENRE_URI_PREFIX + genre.toLowerCase().replace(' ', '_'));
    }

    private Channel processParentChannel(
            Station station,
            org.atlasapi.remotesite.pa.channels.bindings.Channel firstChild,
            Set<String> genres,
            Boolean isAdult
    ) {
        Channel parentChannel = Channel.builder()
                .withUri(STATION_URI_PREFIX + station.getId())
                .withKey(generateStationKey(station.getId()))
                .withSource(Publisher.METABROADCAST)
                .withAvailableFrom(ImmutableList.of(Publisher.PA))
                .withAdult(isAdult)
                .withGenres(genres)
                .build();

        setChannelTitleAndImage(parentChannel, station.getNames().getName(), ImmutableList.of());

        // MediaType and HD flag can't be obtained from the PA Station, so are taken from the
        // first child channel
        // Regional always set to false on channels created from stations
        // Timeshift left as null as this is the channel from which children are considered
        // to be timeshifted
        parentChannel.setMediaType(MEDIA_TYPE_MAPPING.get(firstChild.getMediaType()));
        parentChannel.setHighDefinition(getHighDefinition(firstChild.getFormat()));
        parentChannel.setRegional(false);

        parentChannel.addAliasUrl(createStationUriFromId(station.getId()));

        parentChannel.addAlias(new Alias(PA_CHANNEL_ID_NAMESPACE, station.getId()));

        return parentChannel;
    }

    private String createStationUriFromId(String id) {
        return STATION_ALIAS_PREFIX + id;
    }

    private Channel processStandaloneChannel(
            org.atlasapi.remotesite.pa.channels.bindings.Channel paChannel,
            List<ServiceProvider> serviceProviders,
            Set<String> genres,
            Boolean isAdult
    ) {
        LocalDate startDate = formatter.parseLocalDate(paChannel.getStartDate());

        Channel channel = Channel.builder()
                .withUri(CHANNEL_URI_PREFIX + paChannel.getId())
                .withKey(generateChannelKey(paChannel.getId()))
                .withSource(Publisher.METABROADCAST)
                .withAvailableFrom(ImmutableList.of(Publisher.PA))
                .withStartDate(startDate)
                .withEndDate(null)
                .withAdult(isAdult)
                .withGenres(genres)
                .build();

        if (paChannel.getProviderChannelIds() != null) {
            for (ProviderChannelId providerChannelId : paChannel.getProviderChannelIds()
                    .getProviderChannelId()) {
                lookupAlias(
                        providerChannelId,
                        serviceProviders
                ).ifPresent(channel::addAliasUrl);
            }
        }

        if (paChannel.getUrls() != null) {
            for (Url paUrl : paChannel.getUrls().getUrl()) {
                if (paUrl.getType().equals(SIMULCAST_LINK_TYPE)) {
                    RelatedLink relatedLink = RelatedLink.simulcastLink(paUrl.getvalue()).build();
                    channel.addRelatedLink(relatedLink);
                } else {
                    log.error("Link type {} not supported", paUrl.getType());
                }
            }
        }

        if (paChannel.getMediaType() != null) {
            channel.setMediaType(MEDIA_TYPE_MAPPING.get(paChannel.getMediaType()));
        }

        channel.setHighDefinition(getHighDefinition(paChannel.getFormat()));

        List<Variation> variations = paChannel.getVariation();
        channel.setRegional(variations.stream().anyMatch(IS_REGIONAL));

        getTimeshift(variations).ifPresent(duration -> {
            channel.setTimeshift(duration);
            channel.setIsTimeshifted(true);
        });

        List<Logo> logos;
        if (paChannel.getLogos() != null) {
            logos = paChannel.getLogos().getLogo();
        } else {
            logos = ImmutableList.of();
        }
        setChannelTitleAndImage(channel, paChannel.getNames().getName(), logos);

        channel.addAliasUrl(PaChannelMap.createUriFromId(paChannel.getId()));

        channel.addAlias(new Alias(PA_CHANNEL_ID_NAMESPACE, paChannel.getId()));

        return channel;
    }

    private Optional<Duration> getTimeshift(List<Variation> variations) {
        for (Variation variation : variations) {
            if (variation.getTimeshift() != null) {
                return Optional.of(
                        Duration.standardMinutes(Long.parseLong(variation.getTimeshift()))
                );
            }
        }
        return Optional.empty();
    }

    private Boolean getHighDefinition(@Nullable String format) {
        return format != null && format.equals(FORMAT_HD);
    }

    private Optional<String> lookupAlias(
            ProviderChannelId providerChannelId,
            List<ServiceProvider> serviceProviders
    ) {
        Optional<ServiceProvider> serviceProvider = getServiceProvider(
                providerChannelId.getServiceProviderId(),
                serviceProviders
        );

        if (!serviceProvider.isPresent()) {
            throw new IllegalStateException(
                    String.format(
                            "ServiceProvider with id %s not found in the channel data file",
                            providerChannelId.getServiceProviderId()
                    )
            );
        }

        if (serviceProvider.get().getNames().getName().isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Service Provider with id %s has no name",
                            providerChannelId.getServiceProviderId()
                    )
            );
        }

        String serviceProviderName = Iterables.getOnlyElement(
                serviceProvider.get().getNames().getName()
        ).getvalue();

        if (YOUVIEW_SERVICE_PROVIDERS_TO_ALIAS_PREFIX.containsKey(serviceProviderName)) {
            return Optional.of(youViewAlias(serviceProviderName,
                    //there has been an incident where the id had a space in the end. SUPPORT-2286
                    providerChannelId.getvalue().trim()
            ));
        }

        log.warn(
                "service provider name {} not recognised. Unable to process providerChannelId {}",
                serviceProviderName, providerChannelId
        );

        return Optional.empty();
    }

    private String youViewAlias(String serviceProviderName, String youViewChannelId) {
        return YOUVIEW_SERVICE_PROVIDERS_TO_ALIAS_PREFIX.get(serviceProviderName)
                + youViewChannelId;
    }

    private void setChannelTitleAndImage(Channel channel, List<Name> names, List<Logo> images) {
        for (Name name : names) {
            LocalDate titleStartDate = formatter.parseLocalDate(name.getStartDate());
            if (name.getEndDate() != null) {
                LocalDate titleEndDate = formatter.parseLocalDate(name.getEndDate());
                channel.addTitle(name.getvalue(), titleStartDate, titleEndDate.plusDays(1));
            } else {
                channel.addTitle(
                        name.getvalue(),
                        titleStartDate
                );
            }
        }

        for (Logo logo : images) {
            LocalDate imageStartDate = formatter.parseLocalDate(logo.getStartDate());
            String type = logo.getType();
            Image image = new Image(IMAGE_PREFIX + logo.getvalue());
            ImageTheme theme = type == null
                               ? ImageTheme.LIGHT_OPAQUE
                               : IMAGE_THEME_MAPPING.get(type);
            image.setTheme(theme);
            image.setWidth(Ints.tryParse(logo.getWidth()));
            image.setHeight(Ints.tryParse(logo.getHeight()));
            image.setType(ImageType.LOGO);
            image.setColor(ImageColor.COLOR);

            if (logo.getEndDate() != null) {
                LocalDate imageEndDate = formatter.parseLocalDate(logo.getEndDate());
                channel.addImage(image, imageStartDate, imageEndDate.plusDays(1));
            } else {
                channel.addImage(image, imageStartDate);
            }
        }
    }
}
