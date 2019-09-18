package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

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
import com.metabroadcast.atlas.glycerin.model.DateRange;
import com.metabroadcast.atlas.glycerin.model.Id;
import com.metabroadcast.atlas.glycerin.model.MasterBrand;
import com.metabroadcast.atlas.glycerin.model.Service;
import com.metabroadcast.atlas.glycerin.queries.MasterBrandsMixin;
import com.metabroadcast.atlas.glycerin.queries.MasterBrandsQuery;
import com.metabroadcast.atlas.glycerin.queries.ServiceTypeOption;
import com.metabroadcast.atlas.glycerin.queries.ServicesQuery;
import com.metabroadcast.columbus.telescope.client.ModelWithPayload;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GlycerinNitroChannelAdapter implements NitroChannelAdapter {

    private static final Logger log = LoggerFactory.getLogger(GlycerinNitroChannelAdapter.class);

    public static final String BBC_SERVICE_NAME_SHORT = "bbc:service:name:short";
    public static final String BBC_SERVICE_LOCATOR = "bbc:service:locator";
    public static final String BBC_SERVICE_SID = "bbc:service:sid";

    private static final int MAXIMUM_PAGE_SIZE = 300;
    private static final String DVB = "terrestrial_service_locator";
    private static final String SID = "service_id";
    private static final String BBC_SERVICE_PID = "bbc:service:pid";
    private static final String BBC_MASTERBRAND_MID = "bbc:masterbrand:mid";
    private static final String PID = "pid";
    private static final String NITRO_MASTERBRAND_URI_PREFIX = "http://nitro.bbc.co.uk/masterbrands/";
    private static final String MASTERBRAND = "masterbrand";
    private static final String BBC_MASTERBRAND_ID = "bbc:masterbrand:id";

    private final NitroImageExtractor imageExtractor = new NitroImageExtractor(1024, 576);
    private final Glycerin glycerin;

    private GlycerinNitroChannelAdapter(Glycerin glycerin) {
        this.glycerin = checkNotNull(glycerin);
    }

    public static GlycerinNitroChannelAdapter create(Glycerin glycerin) {
        return new GlycerinNitroChannelAdapter(glycerin);
    }

    @Override
    public ImmutableList<ModelWithPayload<Channel>> fetchServices() throws GlycerinException {
        return fetchServices(ImmutableMap.of());
    }

    @Override
    public ImmutableList<ModelWithPayload<Channel>> fetchServices(
            ImmutableMap<String, Channel> uriToParentChannels)
            throws GlycerinException {

        ImmutableList.Builder<ModelWithPayload<Channel>> channels = ImmutableList.builder();

        ServicesQuery servicesQuery = ServicesQuery.builder()
                .withPageSize(MAXIMUM_PAGE_SIZE)
                .withPage(1)
                .withServiceType(
                        ServiceTypeOption.LOCAL_RADIO,
                        ServiceTypeOption.NATIONAL_RADIO,
                        ServiceTypeOption.REGIONAL_RADIO,
                        ServiceTypeOption.TV
                )
                .build();
        GlycerinResponse<Service> response = glycerin.execute(servicesQuery);

        List<Service> results = response.getResults();

        channels.addAll(extractChannels(uriToParentChannels, results));

        while (response.hasNext()) {
            response = response.getNext();
            results = response.getResults();

            channels.addAll(extractChannels(uriToParentChannels, results));
        }

        return channels.build();
    }

    private ImmutableList<ModelWithPayload<Channel>> extractChannels(
            ImmutableMap<String, Channel> uriToParentChannels,
            List<Service> results
    ) {
        return results.stream()
                .filter(this::isLive)
                .flatMap(service -> generateChannelsFromIds(
                        service,
                        uriToParentChannels
                ).stream())
                .collect(MoreCollectors.toImmutableList());

    }

    private boolean isLive(Service service) {
        DateRange range = service.getDateRange();

        if (range == null) {
            return true;
        }

        XMLGregorianCalendar end = range.getEnd();
        if (end == null) {
            return true;
        }

        DateTime endJoda = new DateTime(end.toGregorianCalendar().getTime());

        return endJoda.isAfter(DateTime.now());
    }

    private ImmutableList<ModelWithPayload<Channel>> generateChannelsFromIds(
            Service result,
            ImmutableMap<String, Channel> uriToParentChannels
    ) {
        boolean hasDvb = result.getIds().getId().stream().anyMatch(id -> DVB.equals(id.getType()));
        if (hasDvb) {
            return result.getIds().getId()
                    .stream()
                    .filter(id -> DVB.equals(id.getType()))
                    .map(id -> makeChannelFromId(result, id, uriToParentChannels))
                    .collect(MoreCollectors.toImmutableList());
        } else {
            return result.getIds().getId()
                    .stream()
                    .filter(id -> SID.equals(id.getType()))
                    .findFirst()
                    .map(id -> makeChannelFromId(result, id, uriToParentChannels))
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        }
    }

    @Override
    public ImmutableSet<ModelWithPayload<Channel>> fetchMasterbrands() throws GlycerinException {
        ImmutableSet.Builder<ModelWithPayload<Channel>> masterBrands = ImmutableSet.builder();

        boolean exhausted = false;
        int startingPoint = 1;
        while (!exhausted) {
            List<MasterBrand> results = paginateMasterBrands(startingPoint);
            for (MasterBrand result : results) {
                Channel channel = getMasterBrand(result);
                channel.setAliases(ImmutableSet.of(new Alias(BBC_MASTERBRAND_MID, result.getMid())));
                masterBrands.add(new ModelWithPayload<Channel>(channel, result));
            }
            startingPoint ++;
            exhausted = results.size() != MAXIMUM_PAGE_SIZE;
        }

        return masterBrands.build();
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

        String title = result.getTitle();
        if (title == null) {
            log.warn(
                    "Found masterbrand {} without title, using name. Could contain non-ASCII characters",
                    result.getMid()
            );
            title = result.getName();
        }

        builder.withTitle(title);

        if (title.toLowerCase().contains("radio") || result.getMid().contains("radio")) {
            builder.withMediaType(MediaType.AUDIO);
        } else {
            builder.withMediaType(MediaType.VIDEO);
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

    private Channel getChannel(Service result, ImmutableMap<String, Channel> parentUriToId) {
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

        if (result.getMasterBrand() != null &&
                !Strings.isNullOrEmpty(result.getMasterBrand().getMid())) {
            setFieldsFromParent(result.getMasterBrand().getMid(), result, builder, parentUriToId);
        }

        Channel channel = builder.build();

        result.getIds().getId().stream()
                .filter(id -> id.getType().equals(PID))
                .forEach(id -> channel.addAlias(new Alias(BBC_SERVICE_PID, id.getValue())));

        return channel;
    }

    private void setFieldsFromParent(
            String parentMid,
            Service child,
            Channel.Builder builder,
            ImmutableMap<String, Channel> parentUriToId
    ) {
        if (!parentUriToId.containsKey(NITRO_MASTERBRAND_URI_PREFIX + parentMid)) {
            log.warn(
                    "Failed to resolve masterbrand parent mid: {} for channel sid: {}",
                    child.getMasterBrand().getMid(),
                    child.getSid()
            );
        } else {
            Set<Alias> aliases = Sets.newHashSet();
            Channel parentChannel = parentUriToId.get(NITRO_MASTERBRAND_URI_PREFIX + parentMid);
            builder.withParent(parentChannel);
            builder.withImages(addMasterbrandAlias(parentChannel.getImages()));
            aliases.add(new Alias(BBC_MASTERBRAND_ID, parentMid));
            if (!Strings.isNullOrEmpty(parentChannel.getTitle())) {
                String shortName = parentChannel.getTitle();
                if (child.getSid().contains("_hd")) {
                    shortName += " HD";
                }
                aliases.add(new Alias(BBC_SERVICE_NAME_SHORT, shortName));
            }
            builder.withAliases(aliases);
        }
    }

    private Iterable<Image> addMasterbrandAlias(Iterable<Image> images) {
        images.forEach(img -> img.addAlias(new Alias(
                NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS,
                MASTERBRAND
        )));
        return images;
    }

    private ModelWithPayload<Channel> makeChannelFromId(
            Service result,
            Id id,
            ImmutableMap<String, Channel> uriToParentChannels
    ) {
        Channel channel = getChannel(result, uriToParentChannels);

        if (id != null && DVB.equals(id.getType())) {
            String locatorValue = id.getValue().toLowerCase();
            String canonicalUri = String.format(
                    "http://nitro.bbc.co.uk/%s/%s_%s",
                    channel.getChannelType() == ChannelType.MASTERBRAND ? "masterbrands" : "services",
                    result.getSid(),
                    locatorValue.replace("dvb://", "").replace("..", "_")
            );

            channel.setCanonicalUri(canonicalUri);

            // Even though it's deprecated all channels must have a key or parts
            // of the code will NPE
            channel.setKey(canonicalUri);

            channel.addAlias(new Alias(BBC_SERVICE_LOCATOR, locatorValue));

            channel.setAliasUrls(ImmutableSet.of(locatorValue));
        }

        channel.addAlias(new Alias(BBC_SERVICE_SID, result.getSid()));

        log.debug("Extracted and totally using Nitro SID {}", result.getSid());

        return new ModelWithPayload<Channel>(channel, result);
    }

    private Optional<LocalDate> getStartDate(Service result) {
        DateRange dateRange = result.getDateRange();
        if (dateRange == null) {
            return Optional.empty();
        }

        XMLGregorianCalendar start = dateRange.getStart();
        if (start == null) {
            return Optional.empty();
        }

        return Optional.of(LocalDate.fromDateFields(start.toGregorianCalendar().getTime()));
    }

    private Optional<LocalDate> getEndDate(Service result) {
        DateRange dateRange = result.getDateRange();
        if (dateRange == null) {
            return Optional.empty();
        }

        XMLGregorianCalendar end = dateRange.getEnd();
        if (end == null) {
            return Optional.empty();
        }

        return Optional.of(LocalDate.fromDateFields(end.toGregorianCalendar().getTime()));
    }

    private Optional<LocalDate> getStartDate(MasterBrand result) {
        MasterBrand.MasterBrandDateRange dateRange = result.getMasterBrandDateRange();
        if (dateRange == null) {
            return Optional.empty();
        }

        XMLGregorianCalendar start = dateRange.getStart();
        if (start == null) {
            return Optional.empty();
        }

        return Optional.of(LocalDate.fromDateFields(start.toGregorianCalendar().getTime()));
    }

    private Optional<LocalDate> getEndDate(MasterBrand result) {
        MasterBrand.MasterBrandDateRange dateRange = result.getMasterBrandDateRange();
        if (dateRange == null) {
            return Optional.empty();
        }

        XMLGregorianCalendar end = dateRange.getEnd();
        if (end == null) {
            return Optional.empty();
        }

        return Optional.of(LocalDate.fromDateFields(end.toGregorianCalendar().getTime()));
    }
}
