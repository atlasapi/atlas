package org.atlasapi.remotesite.bbc.nitro.channels;

import java.io.File;
import java.util.Optional;

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

    public static final String NAME = "name";

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    private static final String BBC_SERVICE_NAME_SHORT = "bbc:service:name:short";
    private static final String BBC_NITRO_TYPE = "bbc:nitro:type";

    private static final String SERVICES_PATH = "/data/youview/sv.json";
    private static final String MASTER_BRAND_PATH = "/data/youview/mb.json";

    private static final String SHORT_NAME = "shortName";
    private static final String DVB_LOCATOR = "dvbLocator";
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

    private static Multimap<String, String> locatorsToTargetRegionInfo;
    private static Table<String, String, String> sidsToValues;
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
        String sid = getSid(channel).get();

        if (sidsToValues.contains(sid, SHORT_NAME)) {
            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            sidsToValues.get(sid, SHORT_NAME)
                    )
            );
        }

        if (sidsToValues.contains(sid, DVB_LOCATOR)
                && channel.getCanonicalUri() == null) {
            String dvb = sidsToValues.get(sid, DVB_LOCATOR);

            String canonicalUri = String.format(
                    "http://nitro.bbc.co.uk/services/%s_%s",
                    sid,
                    dvb.replace("dvb://", "").replace("..", "_")
            );
            channel.setCanonicalUri(canonicalUri);

            channel.addAlias(new Alias(GlycerinNitroChannelAdapter.BBC_SERVICE_LOCATOR, dvb));

            channel.setAliasUrls(ImmutableSet.of(dvb));
        }

        if (!Strings.isNullOrEmpty(sidsToValues.get(sid, IMAGE_IDENT))) {
            overrideIdent(channel, sid, sidsToValues);
        }

        if (locatorsToTargetRegionInfo.containsKey(sid)) {
            channel.setTargetRegions(
                    ImmutableSet.copyOf(locatorsToTargetRegionInfo.get(sid))
            );
        }
        if (sidsToValues.contains(sid, INTERACTIVE)) {
            channel.setInteractive(
                    Boolean.parseBoolean(sidsToValues.get(sid, INTERACTIVE))
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

    private void overrideIdent(
            Channel channel,
            String fieldsKey,
            Table<String, String, String> fields
    ) {
        Image overrideImage = new Image(fields.get(fieldsKey, IMAGE_IDENT));

        overrideImage.setWidth(Integer.parseInt(fields.get(fieldsKey, WIDTH_IDENT)));
        overrideImage.setHeight(Integer.parseInt(fields.get(fieldsKey, HEIGHT_IDENT)));
        overrideImage.setTheme(ImageTheme.LIGHT_OPAQUE);
        overrideImage.setAliases(
                ImmutableSet.of(
                        new Alias(BBC_IMAGE_TYPE, IDENT),
                        new Alias(BBC_IMAGE_TYPE, OVERRIDE)
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

        log.info(
                "Adding override ident {} for {}",
                overrideImage.getCanonicalUri(),
                channel.getUri()
        );

        channel.setImages(images.build());
    }

    private boolean isImageIdent(Alias input) {
        return BBC_NITRO_TYPE.equals(input.getNamespace()) && IDENT.equals(input.getValue());
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

        ImmutableMultimap.Builder<String, String> locatorsToTargetInfoBuilder = ImmutableMultimap.builder();
        ImmutableTable.Builder<String, String, String> locatorsToValuesBuilder = ImmutableTable.builder();

        for (YouviewService service : services) {
            String sid = service.getSid();

            if (Strings.isNullOrEmpty(sid)) {
                continue;
            }

            if (!Strings.isNullOrEmpty(service.getLocator())) {
                locatorsToValuesBuilder.put(sid, DVB_LOCATOR, service.getLocator());
            }

            if (service.getTargets() != null) {
                for (String targetRegion : service.getTargets()) {
                    locatorsToTargetInfoBuilder.put(sid, targetRegion);
                }
            }

            if (!Strings.isNullOrEmpty(service.getName())) {
                locatorsToValuesBuilder.put(sid, NAME, service.getName());
            }

            if (!Strings.isNullOrEmpty(service.getShortName())) {
                locatorsToValuesBuilder.put(sid, SHORT_NAME, service.getShortName());
            }

            if (!Strings.isNullOrEmpty(service.getImage()) && service.getWidth() != null && service.getHeight() != null) {
                locatorsToValuesBuilder.put(sid, IMAGE_IDENT, service.getImage());
                locatorsToValuesBuilder.put(sid, WIDTH_IDENT, service.getWidth().toString());
                locatorsToValuesBuilder.put(sid, HEIGHT_IDENT, service.getHeight().toString());
            }

            if (service.getInteractive() != null) {
                locatorsToValuesBuilder.put(sid, INTERACTIVE, service.getInteractive().toString());
            }

        }

        locatorsToTargetRegionInfo = locatorsToTargetInfoBuilder.build();
        sidsToValues = locatorsToValuesBuilder.build();
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
