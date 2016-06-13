package org.atlasapi.remotesite.bbc.nitro.channels;

import java.io.File;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.remotesite.bbc.nitro.GlycerinNitroChannelAdapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NitroChannelHydrator {

    private static final Logger log = LoggerFactory.getLogger(NitroChannelHydrator.class);

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final String BBC_SERVICE_NAME_SHORT = "bbc:service:name:short";
    private static final String SERVICES_PATH = "/data/youview/sv.json";
    private static final String MASTER_BRAND_PATH = "/data/youview/mb.json";

    public static final String NAME = "name";
    private static final String SHORT_NAME = "shortName";
    private static final String IMAGE_IDENT = "imageIdent";
    private static final String WIDTH_IDENT = "widthIdent";
    private static final String HEIGHT_IDENT = "heightIdent";
    private static final String IMAGE_DOG = "imageDog";
    private static final String WIDTH_DOG = "widthDog";
    private static final String HEIGHT_DOG = "heightDog";
    private static final String INTERACTIVE = "interactive";
    private static final String BBC_IMAGE_TYPE = "bbc:imageType";
    private static final String DOG = "dog";
    private static final String IDENT = "ident";
    private static final String IPLAYER_LOGO = "http://images.atlas.metabroadcast.com/youview.com/201606131640_bbc_iplayer_mono.png";
    private static final String OVERRIDE = "override";
    private static final String BBC_NITRO_TYPE = "bbc:nitro:type";

    private static Multimap<String, String> locatorsToTargetInfo;
    private static Table<String, String, String> locatorsToValues;
    private static Table<String, String, String> masterbrandNamesToValues;

    static {
        populateTables();
    }

    public Iterable<Channel> hydrateServices(Iterable<Channel> services) {
        for (Channel channel : services) {
            try {
                hydrateService(channel);
            } catch (Exception e) {
                log.error(
                        "Failed to hydrate service {} - {}",
                        channel.getUri(),
                        Throwables.getStackTraceAsString(e)
                );
            }
        }
        return services;
    }

    private void hydrateService(Channel channel) {
        String dvbLocator = getDvbLocator(channel).get();
        if (locatorsToValues.contains(dvbLocator, SHORT_NAME)) {
            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            locatorsToValues.get(dvbLocator, SHORT_NAME)
                    )
            );
        }

        if (!Strings.isNullOrEmpty(locatorsToValues.get(dvbLocator, IMAGE_IDENT))) {
            overrideIdent(channel, dvbLocator, locatorsToValues);
        }

        if (locatorsToTargetInfo.containsKey(dvbLocator)) {
            channel.setTargetRegions(
                    ImmutableSet.copyOf(locatorsToTargetInfo.get(dvbLocator))
            );
        }
        if (locatorsToValues.contains(dvbLocator, INTERACTIVE)) {
            channel.setInteractive(
                    Boolean.parseBoolean(locatorsToValues.get(dvbLocator, INTERACTIVE))
            );
        }
    }

    public Iterable<Channel> hydrateMasterbrands(Iterable<Channel> masterbrands) {
        for (Channel channel : masterbrands) {
            try {
                hydrateMasterbrand(channel);
            } catch (Exception e) {
                log.error(
                        "Failed to hydrate masterbrand {} - {}",
                        channel.getUri(),
                        Throwables.getStackTraceAsString(e)
                );
            }
        }
        return masterbrands;
    }

    private void hydrateMasterbrand(Channel channel) {
        String name = channel.getTitle();
        if (masterbrandNamesToValues.contains(name, SHORT_NAME)) {
            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            masterbrandNamesToValues.get(name, SHORT_NAME)
                    )
            );
        }
        if (!Strings.isNullOrEmpty(masterbrandNamesToValues.get(name, IMAGE_IDENT))) {
            overrideIdent(channel, name, masterbrandNamesToValues);
        }

        if (!Strings.isNullOrEmpty(masterbrandNamesToValues.get(name, IMAGE_DOG))) {
            overrideDog(channel, name, masterbrandNamesToValues);
        } else {
            log.info("Adding iplayer image for {}", channel.getCanonicalUri());
            Image iplayerDog = new Image(IPLAYER_LOGO);
            iplayerDog.setHeight(1024);
            iplayerDog.setWidth(169);
            iplayerDog.setAliases(
                    ImmutableSet.of(
                            new Alias(BBC_IMAGE_TYPE, DOG),
                            new Alias(BBC_IMAGE_TYPE, OVERRIDE)
                    )
            );
            channel.addImage(iplayerDog);
        }
    }

    private void overrideIdent(Channel channel, String name, Table<String, String, String> fields) {
        Image overrideImage = new Image(fields.get(name, IMAGE_IDENT));
        overrideImage.setWidth(Integer.parseInt(fields.get(name, WIDTH_IDENT)));
        overrideImage.setHeight(Integer.parseInt(fields.get(name, HEIGHT_IDENT)));
        overrideImage.setTheme(ImageTheme.LIGHT_OPAQUE);
        overrideImage.setAliases(
                ImmutableSet.of(
                        new Alias(BBC_IMAGE_TYPE, IDENT),
                        new Alias(BBC_IMAGE_TYPE, OVERRIDE)
                )
        );

        ImmutableSet.Builder<TemporalField<Image>> images = ImmutableSet.builder();
        for (Image oldImage : channel.getImages()) {
            boolean isIdent = Iterables.any(oldImage.getAliases(), new Predicate<Alias>() {
                @Override
                public boolean apply(@Nullable Alias input) {
                    return BBC_NITRO_TYPE.equals(input.getNamespace()) &&
                            IDENT.equals(input.getValue());
                }
            });
            if (!isIdent) {
                images.add(new TemporalField<>(oldImage, null, null));
            }
        }
        images.add(new TemporalField<>(overrideImage, null, null));
        log.info("Adding override ident {} for {}", overrideImage.getCanonicalUri(), channel.getUri());
        channel.setImages(images.build());
    }

    private void overrideDog(Channel channel, String name, Table<String, String, String> fields) {
        Image overrideImage = new Image(fields.get(name, IMAGE_DOG));
        overrideImage.setWidth(Integer.parseInt(fields.get(name, WIDTH_DOG)));
        overrideImage.setHeight(Integer.parseInt(fields.get(name, HEIGHT_DOG)));
        overrideImage.setTheme(ImageTheme.LIGHT_OPAQUE);
        overrideImage.setAliases(
                ImmutableSet.of(
                        new Alias(BBC_IMAGE_TYPE, DOG),
                        new Alias(BBC_IMAGE_TYPE, OVERRIDE)
                )
        );
        ImmutableSet.Builder<TemporalField<Image>> images = ImmutableSet.builder();
        for (Image oldImage : channel.getImages()) {
            boolean isDog = Iterables.any(oldImage.getAliases(), new Predicate<Alias>() {
                @Override
                public boolean apply(@Nullable Alias input) {
                    return BBC_IMAGE_TYPE.equals(input.getNamespace()) &&
                            DOG.equals(input.getValue());
                }
            });
            if (!isDog) {
                images.add(new TemporalField<>(oldImage, null, null));
            }
        }
        images.add(new TemporalField<>(overrideImage, null, null));
        log.info("Adding override dog {} for {}", overrideImage.getCanonicalUri(), channel.getUri());
        channel.setImages(images.build());
    }

    private static Optional<String> getDvbLocator(Channel channel) {
        for (Alias alias : channel.getAliases()) {
            if (GlycerinNitroChannelAdapter.BBC_SERVICE_LOCATOR.equals(alias.getNamespace())) {
                return Optional.of(alias.getValue());
            }
        }
        return Optional.absent();
    }

    private static void populateTables() {
        ImmutableMultimap.Builder<String, String> locatorsToTargetInfoBuilder = ImmutableMultimap.builder();
        ImmutableTable.Builder<String, String, String> locatorsToValuesBuilder = ImmutableTable.builder();
        ImmutableTable.Builder<String, String, String> masterbrandNamesToValuesBuilder = ImmutableTable.builder();
        try {
            YouviewService[] services = MAPPER.readValue(new File(SERVICES_PATH), YouviewService[].class);
            for (YouviewService service : services) {
                if (!Strings.isNullOrEmpty(service.getLocator())) {
                    if (service.getTargets() != null) {
                        for (String targetRegion : service.getTargets()) {
                            locatorsToTargetInfoBuilder.put(service.getLocator(), targetRegion);
                        }
                    }
                    if (!Strings.isNullOrEmpty(service.getName())) {
                        locatorsToValuesBuilder.put(service.getLocator(), NAME, service.getName());
                    }
                    if (!Strings.isNullOrEmpty(service.getShortName())) {
                        locatorsToValuesBuilder.put(service.getLocator(), SHORT_NAME, service.getShortName());
                    }
                    if (!Strings.isNullOrEmpty(service.getName())) {
                        locatorsToValuesBuilder.put(service.getLocator(), NAME, service.getName());
                    }
                    if (!Strings.isNullOrEmpty(service.getShortName())) {
                        locatorsToValuesBuilder.put(service.getLocator(), SHORT_NAME, service.getShortName());
                    }
                    if (!Strings.isNullOrEmpty(service.getImage()) &&
                            service.getWidth() != null &&
                            service.getHeight() != null) {
                        locatorsToValuesBuilder.put(service.getLocator(), IMAGE_IDENT, service.getImage());
                        locatorsToValuesBuilder.put(service.getLocator(), WIDTH_IDENT, service.getWidth().toString());
                        locatorsToValuesBuilder.put(service.getLocator(), HEIGHT_IDENT, service.getHeight().toString());
                    }
                    if (service.getInteractive() != null) {
                        locatorsToValuesBuilder.put(service.getLocator(), INTERACTIVE, service.getInteractive().toString());
                    }
                }
            }
            locatorsToTargetInfo = locatorsToTargetInfoBuilder.build();
            locatorsToValues = locatorsToValuesBuilder.build();

            YouviewMasterbrand[] masterbrands = MAPPER.readValue(new File(MASTER_BRAND_PATH), YouviewMasterbrand[].class);

            for (YouviewMasterbrand masterbrand : masterbrands) {
                if (!Strings.isNullOrEmpty(masterbrand.getName())) {
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
            }
            masterbrandNamesToValues = masterbrandNamesToValuesBuilder.build();
        } catch (Exception e) {
            log.error("Exception while processing channel configuration", e);
            throw Throwables.propagate(e);
        }
    }

}
