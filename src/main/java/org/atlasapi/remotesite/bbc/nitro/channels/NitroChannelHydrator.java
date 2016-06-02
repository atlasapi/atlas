package org.atlasapi.remotesite.bbc.nitro.channels;

import java.io.File;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
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
    public static final String BBC_SERVICE_NAME_SHORT = "bbc:service:name:short";
    private static final String SERVICES_PATH = "/data/youview/sv.json";
    private static final String MASTER_BRAND_PATH = "/data/youview/mb.json";

    public static final String NAME = "name";
    public static final String SHORT_NAME = "shortName";
    public static final String IMAGE_IDENT = "imageIdent";
    public static final String WIDTH_IDENT = "widthIdent";
    public static final String HEIGHT_IDENT = "heightIdent";
    public static final String IMAGE_DOG = "imageDog";
    public static final String WIDTH_DOG = "widthDog";
    public static final String HEIGHT_DOG = "heightDog";
    public static final String IMAGE = "image";
    public static final String WIDTH = "width";
    public static final String HEIGHT = "height";
    public static final String INTERACTIVE = "interactive";

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
        for (Channel filteredService : filteredServices) {
            // used to be filteredService.getCanonicalUri()
            String dvbLocator = getDvbLocator(filteredService).get();

            filteredService.addAlias(new Alias(
                    BBC_SERVICE_NAME_SHORT,
                    locatorsToValues.get(dvbLocator, SHORT_NAME)
            ));
            Image image = new Image(locatorsToValues.get(dvbLocator, IMAGE));
            image.setWidth(Integer.parseInt(locatorsToValues.get(dvbLocator, WIDTH)));
            image.setHeight(Integer.parseInt(locatorsToValues.get(dvbLocator, HEIGHT)));
            image.setTheme(ImageTheme.LIGHT_OPAQUE);
            filteredService.addImage(image);
            filteredService.setTargetRegions(
                    ImmutableSet.copyOf(locatorsToTargetInfo.get(dvbLocator))
            );
            filteredService.setInteractive(
                    Boolean.parseBoolean(locatorsToValues.get(dvbLocator, INTERACTIVE))
            );
        }
        return filteredServices;
    }

    public Iterable<Channel> filterAndHydrateMasterbrands(Iterable<Channel> masterbrands) {
        Iterable<Channel> filteredMasterbrands = Iterables.filter(masterbrands, inMasterbrandTable());
        for (Channel filteredService : filteredMasterbrands) {
            String name = filteredService.getTitle();
            filteredService.addAlias(new Alias(BBC_SERVICE_NAME_SHORT, masterbrandNamesToValues.get(
                    name, SHORT_NAME)));

            Image identImage = new Image(masterbrandNamesToValues.get(name, IMAGE_IDENT));
            identImage.setWidth(Integer.parseInt(masterbrandNamesToValues.get(name, WIDTH_IDENT)));
            identImage.setHeight(Integer.parseInt(masterbrandNamesToValues.get(name, HEIGHT_IDENT)));
            identImage.setTheme(ImageTheme.LIGHT_OPAQUE);
            identImage.setAliases(
                    ImmutableSet.of(
                            new Alias("bbc:imageType", "ident")
                    )
            );
            filteredService.addImage(identImage);

            Image dogImage = new Image(masterbrandNamesToValues.get(name, IMAGE_DOG));
            dogImage.setWidth(Integer.parseInt(masterbrandNamesToValues.get(name, WIDTH_DOG)));
            dogImage.setHeight(Integer.parseInt(masterbrandNamesToValues.get(name, HEIGHT_DOG)));
            dogImage.setTheme(ImageTheme.LIGHT_OPAQUE);
            dogImage.setAliases(
                    ImmutableSet.of(
                            new Alias("bbc:imageType", "dog")
                    )
            );
            filteredService.addImage(dogImage);
        }
        return filteredMasterbrands;
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
                locatorsToValuesBuilder.put(service.getLocator(), IMAGE, service.getImage());
                locatorsToValuesBuilder.put(service.getLocator(), WIDTH, service.getWidth().toString());
                locatorsToValuesBuilder.put(service.getLocator(), HEIGHT, service.getHeight().toString());
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
