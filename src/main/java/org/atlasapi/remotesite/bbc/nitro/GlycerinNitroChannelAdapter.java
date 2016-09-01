package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroImageExtractor;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Id;
import com.metabroadcast.atlas.glycerin.model.Ids;
import com.metabroadcast.atlas.glycerin.model.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Service;
import com.metabroadcast.atlas.glycerin.queries.MasterBrandsMixin;
import com.metabroadcast.atlas.glycerin.queries.MasterBrandsQuery;
import com.metabroadcast.atlas.glycerin.queries.ServiceTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ServicesQuery;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GlycerinNitroChannelAdapter implements NitroChannelAdapter {

    private static final Logger log = LoggerFactory.getLogger(GlycerinNitroChannelAdapter.class);

    public static final String BBC_SERVICE_NAME_SHORT = "bbc:service:name:short";
    public static final String BBC_SERVICE_LOCATOR = "bbc:service:locator";

    private static final int MAXIMUM_PAGE_SIZE = 300;
    private static final String TERRESTRIAL_SERVICE_LOCATOR = "terrestrial_service_locator";
    private static final String BBC_SERVICE_PID = "bbc:service:pid";
    private static final String BBC_SERVICE_SID = "bbc:service:sid";
    private static final String BBC_MASTERBRAND_MID = "bbc:masterbrand:mid";
    private static final String PID = "pid";
    private static final String NITRO_MASTERBRAND_URI_PREFIX = "http://nitro.bbc.co.uk/masterbrands/";
    private static final String BBC_IMAGE_TYPE = "bbc:imageType";
    private static final String MASTERBRAND = "masterbrand";

    private final NitroImageExtractor imageExtractor = new NitroImageExtractor(1024, 576);
    private final Glycerin glycerin;

    private GlycerinNitroChannelAdapter(Glycerin glycerin) {
        this.glycerin = checkNotNull(glycerin);
    }

    public static GlycerinNitroChannelAdapter create(Glycerin glycerin) {
        return new GlycerinNitroChannelAdapter(glycerin);
    }

    @Override
    public ImmutableSet<Channel> fetchServices() throws GlycerinException {
        return fetchServices(ImmutableMap.of());
    }

    @Override
    public ImmutableSet<Channel> fetchServices(ImmutableMap<String, Channel> uriToParentChannels) throws GlycerinException {
        ImmutableSet.Builder<Channel> channels = ImmutableSet.builder();

        boolean exhausted = false;
        int startingPoint = 1;
        while (!exhausted) {
            List<Service> results = paginateServices(startingPoint);
            for (Service result : results) {
                Ids ids = result.getIds();
                if (ids != null) {
                    generateAndAddChannelsFromLocators(channels, result, ids, uriToParentChannels);
                }
            }
            startingPoint ++;
            exhausted = results.size() != MAXIMUM_PAGE_SIZE;
        }

        return channels.build();
    }

    private void generateAndAddChannelsFromLocators(
            ImmutableSet.Builder<Channel> channels,
            Service result,
            Ids ids,
            ImmutableMap<String, Channel> uriToParentChannels
    ) throws GlycerinException {
        Channel channel;
        for (Id id : ids.getId()) {
            if (id.getType().equals(TERRESTRIAL_SERVICE_LOCATOR)) {
                channel = getChannelWithLocatorAlias(result, id, uriToParentChannels);
                channels.add(channel);
            }
        }
    }

    @Override
    public ImmutableSet<Channel> fetchMasterbrands() throws GlycerinException {
        ImmutableSet.Builder<Channel> masterBrands = ImmutableSet.builder();

        boolean exhausted = false;
        int startingPoint = 1;
        while (!exhausted) {
            List<MasterBrand> results = paginateMasterBrands(startingPoint);
            for (MasterBrand result : results) {
                Channel channel = getMasterBrand(result);
                channel.setAliases(ImmutableSet.of(new Alias(BBC_MASTERBRAND_MID, result.getMid())));
                masterBrands.add(channel);
            }
            startingPoint ++;
            exhausted = results.size() != MAXIMUM_PAGE_SIZE;
        }

        return masterBrands.build();
    }

    private List<Service> paginateServices(int page) throws GlycerinException {
        checkArgument(page > 0, "page count starts with 1");
        ServicesQuery servicesQuery = ServicesQuery.builder()
                .withPageSize(MAXIMUM_PAGE_SIZE)
                .withPage(page)
                .withServiceType(
                        ServiceTypeOption.LOCAL_RADIO, ServiceTypeOption.NATIONAL_RADIO,
                        ServiceTypeOption.REGIONAL_RADIO, ServiceTypeOption.TV)
                .build();
        GlycerinResponse<Service> response = glycerin.execute(servicesQuery);
        return response.getResults();
    }

    private List<MasterBrand> paginateMasterBrands(int page) throws GlycerinException {
        checkArgument(page > 0, "page count starts with 1");
        MasterBrandsQuery masterBrandsQuery = MasterBrandsQuery.builder()
                .withPageSize(MAXIMUM_PAGE_SIZE)
                .withPage(page)
                .withMixins(MasterBrandsMixin.IMAGES)
                .build();
        GlycerinResponse<MasterBrand> response = glycerin.execute(masterBrandsQuery);
        return response.getResults();
    }

    private Channel getMasterBrand(MasterBrand result) {
        String uri = NITRO_MASTERBRAND_URI_PREFIX + result.getMid();

        Channel.Builder builder = Channel.builder()
                .withBroadcaster(Publisher.BBC)
                .withSource(Publisher.BBC_NITRO)
                .withUri(uri)
                .withChannelType(ChannelType.MASTERBRAND);

        // Even though it's deprecated all channels must have a key or parts
        // of the code will NPE
        builder.withKey(uri);

        String name = result.getName();
        if (name != null) {
            builder.withTitle(name);
        } else {
            builder.withTitle(result.getTitle());
        }

        if (result.getSynopses() != null) {
            builder.withShortDescription(result.getSynopses().getShort())
                    .withMediumDescription(result.getSynopses().getMedium())
                    .withLongDescription(result.getSynopses().getLong());
        }

        Optional<LocalDate> startDate = getStartDate(result);
        if (startDate.isPresent()) {
            builder.withStartDate(startDate.get());
        }
        Optional<LocalDate> endDate = getEndDate(result);
        if (endDate.isPresent()) {
            builder.withEndDate(endDate.get());
        }

        if (result.getImages() != null && result.getImages().getImage() != null) {
            Image image = imageExtractor.extract(result.getImages().getImage());
            builder.withImage(image);
        }

        return builder.build();
    }

    private Channel getChannel(Service result, ImmutableMap<String, Channel> parentUriToId) throws GlycerinException {
        Channel.Builder builder = Channel.builder()
                .withBroadcaster(Publisher.BBC)
                .withMediaType(MediaType.valueOf(result.getMediaType().toUpperCase()))
                .withSource(Publisher.BBC_NITRO)
                .withTitle(result.getName())
                .withMediumDescription(result.getDescription())
                .withRegion(result.getRegion())
                .withChannelType(ChannelType.CHANNEL);

        // Even though it's deprecated all channels must have a key or parts
        // of the code will NPE
        builder.withKey(NITRO_MASTERBRAND_URI_PREFIX + result.getSid());

        Optional<LocalDate> startDate = getStartDate(result);
        if (startDate.isPresent()) {
            builder.withStartDate(startDate.get());
        }

        Optional<LocalDate> endDate = getEndDate(result);
        if (endDate.isPresent()) {
            builder.withEndDate(endDate.get());
        }

        if (result.getMasterBrand() != null &&
                !Strings.isNullOrEmpty(result.getMasterBrand().getMid())) {
            setFieldsFromParent(result.getMasterBrand().getMid(), result, builder, parentUriToId);
        }

        Channel channel = builder.build();

        for (Id id : result.getIds().getId()) {
            if (id.getType().equals(PID)) {
                channel.addAlias(new Alias(BBC_SERVICE_PID, id.getValue()));
            }
        }

        return channel;
    }

    private void setFieldsFromParent(
            String parentMid,
            Service child,
            Channel.Builder builder,
            ImmutableMap<String, Channel> parentUriToId
    ) throws GlycerinException {
        if (!parentUriToId.containsKey(NITRO_MASTERBRAND_URI_PREFIX + parentMid)) {
            log.warn(
                    "Failed to resolve masterbrand parent mid: {} for channel sid: {}",
                    child.getMasterBrand().getMid(),
                    child.getSid()
            );
        } else {
            Channel parentChannel = parentUriToId.get(NITRO_MASTERBRAND_URI_PREFIX + parentMid);
            builder.withParent(parentChannel);
            builder.withImages(addMasterbrandAlias(parentChannel.getImages()));
            if (!Strings.isNullOrEmpty(parentChannel.getTitle())) {
                builder.withAliases(
                        ImmutableSet.of(
                                new Alias(BBC_SERVICE_NAME_SHORT, parentChannel.getTitle())
                        )
                );
            }
        }
    }

    private Iterable<Image> addMasterbrandAlias(Iterable<Image> images) {
        return Iterables.transform(images, new Function<Image, Image>() {
                    @Override public Image apply(@Nullable Image input) {
                        input.addAlias(
                                new Alias(BBC_IMAGE_TYPE,  MASTERBRAND)
                        );
                        return input;
                    }
                });
    }

    private Channel getChannelWithLocatorAlias(
            Service result,
            Id locator,
            ImmutableMap<String, Channel> uriToParentChannels
    ) throws GlycerinException {
        Channel channel = getChannel(result, uriToParentChannels);
        String locatorValue = locator.getValue();

        String canonicalUri = String.format(
                "http://nitro.bbc.co.uk/%s/%s_%s",
                channel.getChannelType() == ChannelType.MASTERBRAND ? "masterbrands" : "services",
                result.getSid(),
                locatorValue.replace("dvb://", "").replace("..", "_")
        );
        channel.setCanonicalUri(canonicalUri);

        channel.addAliases(ImmutableSet.of(
                new Alias(BBC_SERVICE_LOCATOR, locatorValue),
                new Alias(BBC_SERVICE_SID, result.getSid())
        ));
        channel.setAliasUrls(ImmutableSet.of(locatorValue));
        return channel;
    }

    private Optional<LocalDate> getStartDate(Service result) {
        try {
            return Optional.of(LocalDate.fromDateFields(result.getDateRange()
                    .getStart()
                    .toGregorianCalendar()
                    .getTime()));
        } catch (NullPointerException e) {
            return Optional.absent();
        }
    }

    private Optional<LocalDate> getEndDate(Service result) {
        try {
            return Optional.of(LocalDate.fromDateFields(result.getDateRange()
                    .getEnd()
                    .toGregorianCalendar()
                    .getTime()));
        } catch (NullPointerException e) {
            return Optional.absent();
        }
    }

    private Optional<LocalDate> getStartDate(MasterBrand result) {
        try {
            return Optional.of(LocalDate.fromDateFields(result.getMasterBrandDateRange()
                    .getStart()
                    .toGregorianCalendar()
                    .getTime()));
        } catch (NullPointerException e) {
            return Optional.absent();
        }
    }

    private Optional<LocalDate> getEndDate(MasterBrand result) {
        try {
            return Optional.of(LocalDate.fromDateFields(result.getMasterBrandDateRange()
                    .getEnd()
                    .toGregorianCalendar()
                    .getTime()));
        } catch (NullPointerException e) {
            return Optional.absent();
        }
    }
}
