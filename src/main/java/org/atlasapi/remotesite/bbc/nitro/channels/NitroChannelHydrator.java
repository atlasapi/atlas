package org.atlasapi.remotesite.bbc.nitro.channels;

import java.io.File;

import javax.annotation.Nullable;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.remotesite.bbc.nitro.GlycerinNitroChannelAdapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
    public static final String IPLAYER_LOGO = "http://www.bbc.co.uk/iplayer/images/youview/bbc_iplayer.png";
    public static final String OVERRIDE = "override";

    private static Multimap<String, String> locatorsToTargetInfo;
    private static Table<String, String, String> locatorsToValues;
    private static Table<String, String, String> masterbrandNamesToValues;

    static {
        populateTables();
    }

    private static final Predicate<Channel> IN_SERVICE_TABLE = new Predicate<Channel>() {

        @Override
        public boolean apply(@Nullable Channel input) {
            Optional<String> locator = getDvbLocator(input);
            return locator.isPresent() && locatorsToTargetInfo.containsKey(locator.get());
        }
    };

    private static final Predicate<Channel> IN_MASTERBRAND_TABLE = new Predicate<Channel>() {

        @Override
        public boolean apply(@Nullable Channel input) {
            return masterbrandNamesToValues.containsRow(input.getTitle());
        }
    };

    private Predicate<Channel> inServiceTable() {
        return IN_SERVICE_TABLE;
    }

    private Predicate<Channel> inMasterbrandTable() {
        return IN_MASTERBRAND_TABLE;
    }

    public Iterable<Channel> filterAndHydrateServices(Iterable<Channel> services) {
        Iterable<Channel> filteredServices = Iterables.filter(services, inServiceTable());

        for (Channel channel : filteredServices) {
            String dvbLocator = getDvbLocator(channel).get();

            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            locatorsToValues.get(dvbLocator, SHORT_NAME)
                    )
            );

            if (!Strings.isNullOrEmpty(locatorsToValues.get(dvbLocator, IMAGE_IDENT))) {
                overrideIdent(channel, dvbLocator, locatorsToValues);
            }

            channel.setTargetRegions(
                    ImmutableSet.copyOf(locatorsToTargetInfo.get(dvbLocator))
            );

            channel.setInteractive(
                    Boolean.parseBoolean(locatorsToValues.get(dvbLocator, INTERACTIVE))
            );
        }

        return filteredServices;
    }

    public Iterable<Channel> filterAndHydrateMasterbrands(Iterable<Channel> masterbrands) {
        Iterable<Channel> filteredMasterbrands = Iterables.filter(masterbrands, inMasterbrandTable());
        for (Channel channel : filteredMasterbrands) {
            String name = channel.getTitle();
            channel.addAlias(
                    new Alias(
                            BBC_SERVICE_NAME_SHORT,
                            masterbrandNamesToValues.get(name, SHORT_NAME)
                    )
            );
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
        return filteredMasterbrands;
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
            for (Alias oldAlias : oldImage.getAliases()) {
                if (!BBC_IMAGE_TYPE.equals(oldAlias.getNamespace()) &&
                        !IDENT.equals(oldAlias.getValue())) {
                    images.add(new TemporalField<>(oldImage, null, null));
                }
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
            for (Alias oldAlias : oldImage.getAliases()) {
                if (!BBC_IMAGE_TYPE.equals(oldAlias.getNamespace()) &&
                        !DOG.equals(oldAlias.getValue())) {
                    images.add(new TemporalField<>(oldImage, null, null));
                }
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
                for (String targetRegion : service.getTargets()) {
                    locatorsToTargetInfoBuilder.put(service.getLocator(), targetRegion);
                }
                locatorsToValuesBuilder.put(service.getLocator(), NAME, service.getName());
                locatorsToValuesBuilder.put(service.getLocator(), SHORT_NAME, service.getShortName());
                locatorsToValuesBuilder.put(service.getLocator(), IMAGE_IDENT, service.getImage());
                locatorsToValuesBuilder.put(service.getLocator(), WIDTH_IDENT, service.getWidth().toString());
                locatorsToValuesBuilder.put(service.getLocator(), HEIGHT_IDENT, service.getHeight().toString());
                locatorsToValuesBuilder.put(service.getLocator(), INTERACTIVE, service.getInteractive().toString());
            }
            locatorsToTargetInfo = locatorsToTargetInfoBuilder.build();
            locatorsToValues = locatorsToValuesBuilder.build();

            YouviewMasterbrand[] masterbrands = MAPPER.readValue(new File(MASTER_BRAND_PATH), YouviewMasterbrand[].class);

            for (YouviewMasterbrand masterbrand : masterbrands) {
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), SHORT_NAME, masterbrand.getShortName());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), IMAGE_IDENT, masterbrand.getImageIdent());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), HEIGHT_IDENT, masterbrand.getHeightIdent().toString());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), WIDTH_IDENT, masterbrand.getWidthIdent().toString());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), IMAGE_DOG, masterbrand.getImageDog());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), HEIGHT_DOG, masterbrand.getHeightDog().toString());
                masterbrandNamesToValuesBuilder.put(masterbrand.getName(), WIDTH_DOG, masterbrand.getWidthDog().toString());
            }
            masterbrandNamesToValues = masterbrandNamesToValuesBuilder.build();
        } catch (Exception e) {
            log.error("Exception while processing channel configuration", e);
            throw Throwables.propagate(e);
        }
    }

}
