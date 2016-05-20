package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GlycerinNitroChannelAdapter implements NitroChannelAdapter {

    private static final int MAXIMUM_PAGE_SIZE = 300;
    private static final String PID = "pid";
    private static final String BBC_SERVICE_PID = "bbc:service:pid";
    private static final String TERRESTRIAL_SERVICE_LOCATOR = "terrestrial_service_locator";
    private static final String BBC_SERVICE_LOCATOR = "bbc:service:locator";
    private static final String NITRO_SERVICE_URI_PREFIX = "http://nitro.bbc.co.uk/service/";
    private static final String NITRO_MASTERBRAND_URI_PREFIX = "http://nitro.bbc.co.uk/masterbrand/";

    private final Glycerin glycerin;

    private GlycerinNitroChannelAdapter(Glycerin glycerin) {
        this.glycerin = checkNotNull(glycerin);
    }

    public static GlycerinNitroChannelAdapter create(Glycerin glycerin) {
        return new GlycerinNitroChannelAdapter(glycerin);
    }

    @Override
    public ImmutableSet<Channel> fetchServices() throws GlycerinException {
        ImmutableSet.Builder<Channel> channels = ImmutableSet.builder();

        boolean exhausted = false;
        int startingPoint = 1;
        while (!exhausted) {
            List<Service> results = paginateServices(startingPoint);
            for (Service result : results) {
                Ids ids = result.getIds();
                if (ids != null) {
                    generateAndAddChannelsFromLocators(channels, result, ids);
                }
            }
            startingPoint ++;
            exhausted = results.size() != MAXIMUM_PAGE_SIZE;
        }

        return channels.build();
    }

    private void generateAndAddChannelsFromLocators(ImmutableSet.Builder<Channel> channels,
            Service result, Ids ids) {
        Channel channel;
        for (Id id : ids.getId()) {
            if (id.getType().equals(TERRESTRIAL_SERVICE_LOCATOR)) {
                channel = getChannelWithLocatorAlias(result, id);
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
                channel.setAliases(ImmutableSet.of(new Alias("bbc:masterbrand:mid", result.getMid())));
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
        Channel.Builder builder = Channel.builder()
                .withBroadcaster(Publisher.BBC)
                .withSource(Publisher.BBC_NITRO)
                .withUri(NITRO_MASTERBRAND_URI_PREFIX + result.getMid())
                .withChannelType(ChannelType.MASTERBRAND);

        String name = result.getName();
        if (name != null) {
            builder.withTitle(name);
            inferAndSetMediaType(builder, name);
        } else {
            String title = result.getTitle();
            inferAndSetMediaType(builder, title);
            builder.withTitle(title);
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
        return builder.build();
    }

    private void inferAndSetMediaType(Channel.Builder builder, String name) {
        if (name.toLowerCase().contains("music") || name.toLowerCase().contains("radio")) {
            builder.withMediaType(MediaType.AUDIO);
        } else {
            builder.withMediaType(MediaType.VIDEO);
        }
    }

    private Channel getChannel(Service result) {
        Channel.Builder builder = Channel.builder()
                .withBroadcaster(Publisher.BBC)
                .withMediaType(MediaType.valueOf(result.getMediaType().toUpperCase()))
                .withSource(Publisher.BBC_NITRO)
                .withTitle(result.getName())
                .withMediumDescription(result.getDescription())
                .withRegion(result.getRegion())
                .withChannelType(ChannelType.CHANNEL);
        Optional<LocalDate> startDate = getStartDate(result);
        if (startDate.isPresent()) {
            builder.withStartDate(startDate.get());
        }
        Optional<LocalDate> endDate = getEndDate(result);
        if (endDate.isPresent()) {
            builder.withEndDate(endDate.get());
        }
        Channel channel = builder.build();
        for (Id id : result.getIds().getId()) {
            if (id.getType().equals(PID)) {
                channel.addAlias(new Alias(BBC_SERVICE_PID, id.getValue()));
            }
        }
        return channel;
    }

    private Channel getChannelWithLocatorAlias(Service result, Id locator) {
        Channel channel = getChannel(result);
        String locatorValue = locator.getValue();
        channel.setCanonicalUri(locatorValue);
        channel.setAliases(ImmutableSet.of(new Alias(BBC_SERVICE_LOCATOR, locatorValue), new Alias("bbc:service:sid", result.getSid())));
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
