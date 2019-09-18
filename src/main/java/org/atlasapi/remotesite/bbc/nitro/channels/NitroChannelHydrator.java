package org.atlasapi.remotesite.bbc.nitro.channels;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.remotesite.bbc.nitro.GlycerinNitroChannelAdapter;
import org.atlasapi.remotesite.bbc.nitro.channels.hax.LocatorWithRegions;
import org.atlasapi.remotesite.bbc.nitro.channels.hax.YouviewMasterbrand;
import org.atlasapi.remotesite.bbc.nitro.channels.hax.YouviewService;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroImageExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

public class NitroChannelHydrator {

    private static final Logger log = LoggerFactory.getLogger(NitroChannelHydrator.class);

    public static final String NAME = "name";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BBC_SERVICE_NAME_SHORT = "bbc:service:name:short";

    private static final String SERVICES_PATH = "/data/youview/sv.json";
    private static final String MASTER_BRAND_PATH = "/data/youview/mb.json";

    private static final String SHORT_NAME = "shortName";
    private static final String IMAGE_IDENT = "imageIdent";
    private static final String WIDTH_IDENT = "widthIdent";
    private static final String HEIGHT_IDENT = "heightIdent";
    private static final String IMAGE_DOG = "imageDog";
    private static final String WIDTH_DOG = "widthDog";
    private static final String HEIGHT_DOG = "heightDog";
    private static final String INTERACTIVE = "interactive";
    private static final String DOG = "dog";
    private static final String IDENT = "ident";
    private static final String BBC_BLOCKS_IDENT = "http://images.atlas.metabroadcast.com/youview.com/201608121530_bbc_block_mono_cropped.png";
    private static final int BBC_BLOCKS_IDENT_WIDTH = 1024;
    private static final int BBC_BLOCKS_IDENT_HEIGHT = 321;
    private static final String IPLAYER_LOGO = "http://images.atlas.metabroadcast.com/youview.com/201606131640_bbc_iplayer_mono.png";
    private static final String OVERRIDE = "override";
    private static final String DVB_LOCATOR = "locator";

    private static Multimap<String, String> sidsToTargetRegionInfo;
    private static Multimap<String, LocatorWithRegions> sidsToLocatorsAndRegions;
    private static Table<String, String, String> sidsToValues;
    private static Table<String, String, String> masterbrandNamesToValues;

    static {
        populateTables();
    }

    public Iterable<ModelWithPayload<Channel>> hydrateServices(Iterable<ModelWithPayload<Channel>> services) {
        ImmutableList.Builder<ModelWithPayload<Channel>> channels = ImmutableList.builder();

        for (ModelWithPayload<Channel> channelWithPayload : services) {
            try {
                channels.addAll(hydrateService(channelWithPayload));
            } catch (Exception e) {
                log.error("Failed to hydrate service " + channelWithPayload.getModel().getUri(), e);
            }
        }

        return channels.build();
    }

    private Iterable<ModelWithPayload<Channel>> hydrateService(ModelWithPayload<Channel> channelWithPayload) {
        ImmutableList.Builder<ModelWithPayload<Channel>> result = ImmutableList.builder();

        Channel channel = channelWithPayload.getModel();
        String sid = getSid(channel).get();

        if (sidsToValues.contains(sid, SHORT_NAME)) {
            String shortName = sidsToValues.get(sid, SHORT_NAME);

            log.debug("Overriding short name for {} to {}", sid, shortName);

            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            shortName
                    )
            );
        }

        String imageIdent = sidsToValues.get(sid, IMAGE_IDENT);
        if (!Strings.isNullOrEmpty(imageIdent)) {
            log.debug("Overriding images for {} to {}", sid, imageIdent);

            overrideIdent(
                    channel,
                    sidsToValues.get(sid, IMAGE_IDENT),
                    Integer.parseInt(sidsToValues.get(sid, WIDTH_IDENT)),
                    Integer.parseInt(sidsToValues.get(sid, HEIGHT_IDENT))
            );
        }

        if (sidsToTargetRegionInfo.containsKey(sid)) {
            Collection<String> regions = sidsToTargetRegionInfo.get(sid);

            log.debug("Overriding region info for {} to {}", sid, regions);

            channel.setTargetRegions(
                    ImmutableSet.copyOf(regions)
            );
        }

        if (sidsToValues.contains(sid, INTERACTIVE)) {
            String interactive = sidsToValues.get(sid, INTERACTIVE);

            log.debug("Overriding interactive for {} to {}", sid, interactive);

            channel.setInteractive(
                    Boolean.parseBoolean(interactive)
            );
        }

        if (sidsToLocatorsAndRegions.containsKey(sid)) {
            Collection<LocatorWithRegions> locatorsWithRegions = sidsToLocatorsAndRegions.get(sid);

            if (channel.getCanonicalUri() == null) {
                for (LocatorWithRegions locatorWithRegions : locatorsWithRegions) {
                    Channel copy = Channel.builder(channel).build();

                    String dvb = locatorWithRegions.getLocator().toLowerCase();
                    List<String> regions = locatorWithRegions.getRegions();

                    setChannelDvbData(sid, copy, dvb);

                    copy.setTargetRegions(ImmutableSet.copyOf(regions));

                    log.debug(
                            "Overriding DVB and regions for {} to {} / {}",
                            sid,
                            dvb,
                            regions
                    );

                    result.add(new ModelWithPayload<>(copy, channelWithPayload.getPayload()));
                }
            } else {
                // they actually added the DVB, but we still need the regions
                Optional<Alias> channelDvbAlias = channel.getAliases()
                        .stream()
                        .filter(alias -> GlycerinNitroChannelAdapter.BBC_SERVICE_LOCATOR.equals(
                                alias.getNamespace()))
                        .findFirst();

                if (!channelDvbAlias.isPresent()) {
                    log.warn("Channel with a canonical URI but no DVB? wat? {}", channelWithPayload);
                } else {
                    Optional<LocatorWithRegions> override = locatorsWithRegions.stream()
                            .filter(lwr -> lwr.getLocator().equals(channelDvbAlias.get().getValue()))
                            .findFirst();
                    if (!override.isPresent()) {
                        log.warn("Found no regions override for DVB {}", channelDvbAlias.get());
                    } else {
                        channel.setTargetRegions(ImmutableSet.copyOf(override.get().getRegions()));
                        result.add(channelWithPayload);
                    }
                }
            }
        } else {
            if (sidsToValues.contains(sid, DVB_LOCATOR)) {
                String dvb = sidsToValues.get(sid, DVB_LOCATOR).toLowerCase();
                setChannelDvbData(sid, channelWithPayload.getModel(), dvb);

                log.debug(
                        "Overriding DVB for {} to {}",
                        sid,
                        dvb
                );
            }

            result.add(channelWithPayload);
        }

        ImmutableList<ModelWithPayload<Channel>> channels = result.build();

        log.debug("Hydrated channels {}", channels);

        return channels;
    }

    private static void setChannelDvbData(String sid, Channel channel, String dvb) {
        String canonicalUri = String.format(
                "http://nitro.bbc.co.uk/services/%s_%s",
                sid,
                dvb.replace("dvb://", "").replace("..", "_")
        );
        channel.setCanonicalUri(canonicalUri);
        channel.setKey(canonicalUri);

        channel.addAlias(new Alias(GlycerinNitroChannelAdapter.BBC_SERVICE_LOCATOR, dvb));

        channel.setAliasUrls(ImmutableSet.of(dvb));
    }

    public Iterable<ModelWithPayload<Channel>> hydrateMasterbrands(Iterable<ModelWithPayload<Channel>> masterbrands) {

        ImmutableList.Builder<ModelWithPayload<Channel>> result = ImmutableList.builder();
        for (ModelWithPayload<Channel> channelWithPayload : masterbrands) {
            try {
                Channel hydrated = hydrateMasterbrand(channelWithPayload.getModel());
                result.add(new ModelWithPayload<>(hydrated, channelWithPayload.getPayload()));
            } catch (Exception e) {
                log.error(
                        "Failed to hydrate masterbrand " + channelWithPayload.getModel().getUri(), e);
            }
        }

        return result.build();
    }

    private Channel hydrateMasterbrand(Channel channel) {
        String name = channel.getTitle();
        if (masterbrandNamesToValues.contains(name, SHORT_NAME)) {
            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            masterbrandNamesToValues.get(name, SHORT_NAME)
                    )
            );
        }

        boolean identIsStock = channel.getImages()
                .stream()
                .filter(img -> img.getAliases()
                        .contains(new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, IDENT))
                ).findFirst()
                .map(img -> img.getCanonicalUri().endsWith("p028s846.jpg")
                        || img.getCanonicalUri().endsWith("p028s846.png"))
                .orElse(false);

        String identOverride = masterbrandNamesToValues.get(name, IMAGE_IDENT);
        if (!Strings.isNullOrEmpty(identOverride)) {
            overrideIdent(
                    channel,
                    masterbrandNamesToValues.get(name, IMAGE_IDENT),
                    Integer.parseInt(masterbrandNamesToValues.get(name, WIDTH_IDENT)),
                    Integer.parseInt(masterbrandNamesToValues.get(name, HEIGHT_IDENT))
            );
        } else if (identIsStock || channel.getImages().isEmpty()) {
            overrideIdent(
                    channel,
                    BBC_BLOCKS_IDENT,
                    BBC_BLOCKS_IDENT_WIDTH,
                    BBC_BLOCKS_IDENT_HEIGHT
            );
        }


        String dogOverride = masterbrandNamesToValues.get(name, IMAGE_DOG);
        if (!Strings.isNullOrEmpty(dogOverride)) {
            overrideDog(channel, name, masterbrandNamesToValues);
        } else {
            log.info("Adding iplayer image for {}", channel.getCanonicalUri());
            Image iplayerDog = new Image(IPLAYER_LOGO);
            iplayerDog.setHeight(169);
            iplayerDog.setWidth(1024);
            iplayerDog.setAliases(
                    ImmutableSet.of(
                            new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, DOG),
                            new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, OVERRIDE)
                    )
            );
            channel.addImage(iplayerDog);
        }

        return channel;
    }

    private void overrideIdent(Channel channel, String imageIdent, int width, int height) {
        Image overrideImage = new Image(imageIdent);

        overrideImage.setWidth(width);
        overrideImage.setHeight(height);
        overrideImage.setTheme(ImageTheme.LIGHT_OPAQUE);
        overrideImage.setAliases(
                ImmutableSet.of(
                        new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, IDENT),
                        new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, OVERRIDE)
                )
        );

        ImmutableSet.Builder<TemporalField<Image>> images = ImmutableSet.builder();

        for (Image oldImage : channel.getImages()) {
            boolean isIdent = oldImage.getAliases()
                    .stream()
                    .anyMatch(this::isImageIdent);

            if (!isIdent) {
                images.add(new TemporalField<>(oldImage, null, null));
            }
        }

        images.add(new TemporalField<>(overrideImage, null, null));

        channel.setImages(images.build());
    }

    private boolean isImageIdent(Alias input) {
        return NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS.equals(input.getNamespace())
                && IDENT.equals(input.getValue());
    }

    private void overrideDog(Channel channel, String name, Table<String, String, String> fields) {
        Image overrideImage = new Image(fields.get(name, IMAGE_DOG));
        overrideImage.setWidth(Integer.parseInt(fields.get(name, WIDTH_DOG)));
        overrideImage.setHeight(Integer.parseInt(fields.get(name, HEIGHT_DOG)));
        overrideImage.setTheme(ImageTheme.LIGHT_OPAQUE);
        overrideImage.setAliases(
                ImmutableSet.of(
                        new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, DOG),
                        new Alias(NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS, OVERRIDE)
                )
        );
        ImmutableSet.Builder<TemporalField<Image>> images = ImmutableSet.builder();
        for (Image oldImage : channel.getImages()) {
            boolean isDog = oldImage.getAliases()
                    .stream()
                    .anyMatch(NitroChannelHydrator::isDog);

            if (!isDog) {
                images.add(new TemporalField<>(oldImage, null, null));
            }
        }
        images.add(new TemporalField<>(overrideImage, null, null));
        log.info("Adding override dog {} for {}", overrideImage.getCanonicalUri(), channel.getUri());
        channel.setImages(images.build());
    }

    private static boolean isDog(Alias alias) {
        return NitroImageExtractor.BBC_NITRO_IMAGE_TYPE_NS.equals(alias.getNamespace())
                && DOG.equals(alias.getValue());
    }

    private static Optional<String> getSid(Channel channel) {
        Optional<Alias> sidAlias = channel.getAliases()
                .stream()
                .filter(alias -> GlycerinNitroChannelAdapter.BBC_SERVICE_SID.equals(alias.getNamespace()))
                .findFirst();

        return sidAlias.flatMap(alias -> Optional.of(alias.getValue()));
    }

    private static void populateTables() {
        try {
            populateServiceTables();
            populateMasterbrandTables();
        } catch (Exception e) {
            log.error("Exception while processing channel configuration", e);
            throw Throwables.propagate(e);
        }
    }

    private static void populateServiceTables() throws java.io.IOException {
        YouviewService[] services = MAPPER.readValue(new File(SERVICES_PATH), YouviewService[].class);

        ImmutableMultimap.Builder<String, LocatorWithRegions> sidsToLocatorsBuilder = ImmutableMultimap.builder();
        ImmutableMultimap.Builder<String, String> sidsToTargetInfoBuilder = ImmutableMultimap.builder();
        ImmutableTable.Builder<String, String, String> sidsToValuesBuilder = ImmutableTable.builder();

        for (YouviewService service : services) {
            String sid = service.getSid();

            if (Strings.isNullOrEmpty(sid)) {
                continue;
            }

            String locator = service.getLocator();
            if (!Strings.isNullOrEmpty(locator)) {
                sidsToValuesBuilder.put(sid, DVB_LOCATOR, locator);
            }

            List<LocatorWithRegions> locators = service.getLocators();
            if (locators != null && ! locators.isEmpty()) {
                sidsToLocatorsBuilder.putAll(sid, locators);
            }

            if (service.getTargets() != null) {
                for (String targetRegion : service.getTargets()) {
                    sidsToTargetInfoBuilder.put(sid, targetRegion);
                }
            }

            if (!Strings.isNullOrEmpty(service.getName())) {
                sidsToValuesBuilder.put(sid, NAME, service.getName());
            }

            if (!Strings.isNullOrEmpty(service.getShortName())) {
                sidsToValuesBuilder.put(sid, SHORT_NAME, service.getShortName());
            }

            if (!Strings.isNullOrEmpty(service.getImage()) && service.getWidth() != null && service.getHeight() != null) {
                sidsToValuesBuilder.put(sid, IMAGE_IDENT, service.getImage());
                sidsToValuesBuilder.put(sid, WIDTH_IDENT, service.getWidth().toString());
                sidsToValuesBuilder.put(sid, HEIGHT_IDENT, service.getHeight().toString());
            }

            if (service.getInteractive() != null) {
                sidsToValuesBuilder.put(sid, INTERACTIVE, service.getInteractive().toString());
            }

        }

        sidsToLocatorsAndRegions = sidsToLocatorsBuilder.build();
        sidsToTargetRegionInfo = sidsToTargetInfoBuilder.build();
        sidsToValues = sidsToValuesBuilder.build();
    }

    private static void populateMasterbrandTables() throws java.io.IOException {
        YouviewMasterbrand[] masterbrands = MAPPER.readValue(new File(MASTER_BRAND_PATH), YouviewMasterbrand[].class);

        ImmutableTable.Builder<String, String, String> masterbrandNamesToValuesBuilder = ImmutableTable.builder();

        for (YouviewMasterbrand masterbrand : masterbrands) {
            if (Strings.isNullOrEmpty(masterbrand.getName())) {
                continue;
            }

            if (!Strings.isNullOrEmpty(masterbrand.getShortName())) {
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), SHORT_NAME, masterbrand.getShortName());
            }

            if (!Strings.isNullOrEmpty(masterbrand.getImageIdent()) &&
                    masterbrand.getWidthIdent() != null &&
                    masterbrand.getHeightIdent() != null) {

                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), IMAGE_IDENT, masterbrand.getImageIdent());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), HEIGHT_IDENT, masterbrand.getHeightIdent().toString());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), WIDTH_IDENT, masterbrand.getWidthIdent().toString());
            }

            if (!Strings.isNullOrEmpty(masterbrand.getImageDog()) &&
                    masterbrand.getHeightDog() != null &&
                    masterbrand.getWidthDog() != null) {

                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), IMAGE_DOG, masterbrand.getImageDog());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), HEIGHT_DOG, masterbrand.getHeightDog().toString());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), WIDTH_DOG, masterbrand.getWidthDog().toString());
            }
        }
        masterbrandNamesToValues = masterbrandNamesToValuesBuilder.build();
    }

}
