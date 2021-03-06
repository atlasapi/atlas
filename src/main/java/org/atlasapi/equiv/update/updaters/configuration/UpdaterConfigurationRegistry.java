package org.atlasapi.equiv.update.updaters.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static java.lang.String.format;
import static org.atlasapi.equiv.update.handlers.types.ContainerEquivalenceHandlerType.NOP_CONTAINER_HANDLER;
import static org.atlasapi.equiv.update.handlers.types.ContainerEquivalenceHandlerType.STANDARD_CONTAINER_HANDLER;
import static org.atlasapi.equiv.update.handlers.types.ContainerEquivalenceHandlerType.STANDARD_NO_EPISODE_MATCHING_CONTAINER_HANDLER;
import static org.atlasapi.equiv.update.handlers.types.ContainerEquivalenceHandlerType.STANDARD_SERIES_HANDLER;
import static org.atlasapi.equiv.update.handlers.types.ItemEquivalenceHandlerType.NOP_ITEM_HANDLER;
import static org.atlasapi.equiv.update.handlers.types.ItemEquivalenceHandlerType.STANDARD_ITEM_HANDLER;
import static org.atlasapi.equiv.update.handlers.types.ItemEquivalenceHandlerType.STRICT_EPISODE_ITEM_HANDLER;
import static org.atlasapi.equiv.update.messagers.types.ContainerEquivalenceMessengerType.NOP_CONTAINER_MESSENGER;
import static org.atlasapi.equiv.update.messagers.types.ContainerEquivalenceMessengerType.STANDARD_CONTAINER_MESSENGER;
import static org.atlasapi.equiv.update.messagers.types.ContainerEquivalenceMessengerType.STANDARD_SERIES_MESSENGER;
import static org.atlasapi.equiv.update.messagers.types.ItemEquivalenceMessengerType.NOP_ITEM_MESSENGER;
import static org.atlasapi.equiv.update.messagers.types.ItemEquivalenceMessengerType.STANDARD_ITEM_MESSENGER;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.DEFAULT_EQUIV_SOURCES;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.MUSIC_SOURCES;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.TARGET_SOURCES;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.VF_SOURCES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.AMAZON_AMAZON_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.AMAZON_AMAZON_SERIES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.AMAZON_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.AMAZON_SERIES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.BROADCAST_ITEM_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.BT_VOD_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.FACEBOOK_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.IMDB_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.IMDB_PA_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.IMDB_PA_SERIES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.IMDB_SERIES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.NOP_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.RTE_VOD_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.RT_UPCOMING_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.STANDARD_SERIES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.STANDARD_TOP_LEVEL_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.VOD_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType.WIKIPEDIA_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.*;
import static org.atlasapi.media.entity.Publisher.*;

/**
 * This class contains the source configuration for equivalence. When the equivalence executor
 * picks up an item, it will check its source to see if there is a specific configuration for it,
 * and if there is it will only match it against the specified sources.
 * <p>
 * WHEN CREATING A NEW CONFIG, keep in mind to remove your source from the DEFAULT_EQUIV_SOURCE list if present.
 * If you don't, standard equivalence will run on top of whatever config you have set, and
 * might equiv to undesired sources.
 */
public class UpdaterConfigurationRegistry {

    private final ImmutableList<UpdaterConfiguration> updaterConfigurations;

    private UpdaterConfigurationRegistry() {
        this.updaterConfigurations = makeConfigurations();
    }

    public static UpdaterConfigurationRegistry create() {
        return new UpdaterConfigurationRegistry();
    }

    public ImmutableList<UpdaterConfiguration> getUpdaterConfigurations() {
        return updaterConfigurations;
    }

    private static ImmutableList<UpdaterConfiguration> makeConfigurations() {
        ImmutableList.Builder<UpdaterConfiguration> configurations = ImmutableList.builder();

        configurations.addAll(makeDefaultConfigurations());

        configurations.add(
                makeRadioTimesUpcomingConfiguration(),
                makeEbsConfiguration(),
                makeAmcEbsConfiguration(),
                makeRadioTimesConfiguration(),
                makeBbcReduxConfiguration(),
                makeC4PressConfiguration(),
                makeBettyConfiguration(),
                makeFacebookConfiguration(),
                makeITunesConfiguration(),
                makeLovefilmConfiguration(),
                makeNetflixConfiguration(),
                makeAmazonUnboxConfiguration(),
                makeTalkTalkConfiguration(),
                makeRteConfiguration(),
                makeFiveConfiguration(),
                makeBarbMasterConfiguration(), //CDMF
                makeBarbTransmissionConfiguration(),
                makeLayer3TxlogsConfiguration(),
                makeAeConfiguration(),
                makeItvCpsConfiguration(),
                makeNitroConfiguration(),
                makeC4PmlsdConfiguration(),
                makeUktvConfiguration(),
                makeWikipediaConfiguration(),
                makeBarbXMasterConfiguration(), //X-CDMF
                makeImdbConfiguration(),
                makeJustWatchConfiguration(),
                makeC5DataSubmissionConfiguration(),
                makeBarbCensusConfiguration(),
                makeBarbNleConfiguration(),
                makeViacom18DataSubmissionConfiguration(),
                makeVimnDataSubmissionConfiguration()
        );

        configurations.add(
                makeBtVodConfiguration(),
                makeBtTveVodConfiguration()
        );

        configurations.add(
                makeYouviewConfiguration(),
                makeYouviewStageConfiguration(),
                makeYouviewBtConfiguration(),
                makeYouviewBtStageConfiguration(),
                makeYouviewScotlandRadioConfiguration(),
                makeYouviewScotlandRadioStageConfiguration()
        );

        configurations.addAll(
                makeVfConfigurations()
        );

        configurations.addAll(
                makeRoviConfigurations()
        );

        configurations.addAll(
                makeMusicConfigurations()
        );

        ImmutableList<UpdaterConfiguration> builtConfigurations = configurations.build();

        validate(builtConfigurations);

        return builtConfigurations;
    }

    private static void validate(ImmutableList<UpdaterConfiguration> configurations) {
        Set<Publisher> configuredSources = Sets.newHashSet();

        for (UpdaterConfiguration configuration : configurations) {
            if (configuredSources.contains(configuration.getSource())) {
                throw new IllegalArgumentException(format(
                        "Found duplicate updater configuration for source %s",
                        configuration.getSource()
                ));
            }

            configuredSources.add(configuration.getSource());
        }
    }

    private static ImmutableList<UpdaterConfiguration> makeDefaultConfigurations() {
        return DEFAULT_EQUIV_SOURCES.stream()
                .map(UpdaterConfigurationRegistry::makeDefaultConfiguration)
                .collect(MoreCollectors.toImmutableList());
    }

    private static UpdaterConfiguration makeDefaultConfiguration(Publisher publisher) {
        return UpdaterConfiguration.builder()
                .withSource(publisher)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_ITEM, MoreSets.add(TARGET_SOURCES, LOVEFILM)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, MoreSets.add(TARGET_SOURCES, LOVEFILM)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeWikipediaConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(WIKIPEDIA)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                WIKIPEDIA_ITEM, ImmutableSet.of(
                                        BBC_NITRO, ITV_CPS, UKTV, C4_PMLSD, C5_DATA_SUBMISSION,
                                        BARB_OVERRIDES, BARB_TRANSMISSIONS, BARB_MASTER
                                )
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                WIKIPEDIA_CONTAINER, ImmutableSet.of(
                                        BBC_NITRO, ITV_CPS, UKTV, C4_PMLSD, C5_DATA_SUBMISSION, BARB_OVERRIDES
                                )
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                WIKIPEDIA_CONTAINER, ImmutableSet.of(
                                        BBC_NITRO, ITV_CPS, UKTV, C4_PMLSD, C5_DATA_SUBMISSION, BARB_OVERRIDES
                                )
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBarbXMasterConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BARB_X_MASTER)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_X_ITEM, ImmutableSet.of(BARB_MASTER)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of()
                        ), //there are no
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of()
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeImdbConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(IMDB)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                IMDB_ITEM, ImmutableSet.of(AMAZON_UNBOX, JUSTWATCH, RADIO_TIMES),
                                ITEM_SEQUENCE, ImmutableSet.of(PA, AMAZON_UNBOX, JUSTWATCH),
                                ITEM_SEARCH, ImmutableSet.of(PA, AMAZON_UNBOX, JUSTWATCH)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                IMDB_PA_CONTAINER, ImmutableSet.of(PA),
                                IMDB_CONTAINER, ImmutableSet.of(AMAZON_UNBOX, JUSTWATCH)
                        ),
                        STANDARD_NO_EPISODE_MATCHING_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                IMDB_PA_SERIES, ImmutableSet.of(PA),
                                IMDB_SERIES, ImmutableSet.of(AMAZON_UNBOX, JUSTWATCH)
                        ),
                        STANDARD_NO_EPISODE_MATCHING_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeJustWatchConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(JUSTWATCH)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                IMDB_ITEM, ImmutableSet.of(AMAZON_UNBOX, IMDB, RADIO_TIMES),
                                ITEM_SEQUENCE, ImmutableSet.of(AMAZON_UNBOX, IMDB),
                                ITEM_SEARCH, ImmutableSet.of(AMAZON_UNBOX, IMDB)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                IMDB_CONTAINER, ImmutableSet.of(AMAZON_UNBOX, IMDB)
                        ),
                        STANDARD_NO_EPISODE_MATCHING_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                IMDB_SERIES, ImmutableSet.of(AMAZON_UNBOX, IMDB)
                        ),
                        STANDARD_NO_EPISODE_MATCHING_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeRadioTimesUpcomingConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(RADIO_TIMES_UPCOMING)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                RT_UPCOMING_ITEM, ImmutableSet.of(AMAZON_UNBOX, PA)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                RT_UPCOMING_CONTAINER, ImmutableSet.of(AMAZON_UNBOX, PA)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                RT_UPCOMING_CONTAINER, ImmutableSet.of(AMAZON_UNBOX, PA)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeEbsConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BT_SPORT_EBS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                STRICT_ITEM, MoreSets.add(TARGET_SOURCES, LOVEFILM)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, MoreSets.add(TARGET_SOURCES, LOVEFILM)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeAmcEbsConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(AMC_EBS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_ITEM, MoreSets.add(TARGET_SOURCES, LOVEFILM)

                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, MoreSets.add(TARGET_SOURCES, LOVEFILM)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of(AMC_EBS, PA)
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeRadioTimesConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(RADIO_TIMES)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                RT_ITEM, ImmutableSet.of(PREVIEW_NETWORKS, AMAZON_UNBOX),
                                IMDB_ITEM, ImmutableSet.of(IMDB, JUSTWATCH)    //equiv via IMDb id
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeYouviewConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                Sets.difference(
                        TARGET_SOURCES,
                        ImmutableSet.of(YOUVIEW_STAGE)
                ),
                ImmutableSet.of(YOUVIEW)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(YOUVIEW)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                YOUVIEW_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeYouviewStageConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                Sets.difference(
                        TARGET_SOURCES,
                        ImmutableSet.of(YOUVIEW)
                ),
                ImmutableSet.of(YOUVIEW_STAGE)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(YOUVIEW_STAGE)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                YOUVIEW_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeYouviewBtConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                Sets.difference(
                        TARGET_SOURCES,
                        ImmutableSet.of(YOUVIEW_BT_STAGE)
                ),
                ImmutableSet.of(YOUVIEW_BT)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(YOUVIEW_BT)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                YOUVIEW_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeYouviewBtStageConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                Sets.difference(
                        TARGET_SOURCES,
                        ImmutableSet.of(YOUVIEW_BT)
                ),
                ImmutableSet.of(YOUVIEW_BT_STAGE)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(YOUVIEW_BT_STAGE)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                YOUVIEW_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeYouviewScotlandRadioConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                Sets.difference(
                        TARGET_SOURCES,
                        ImmutableSet.of(YOUVIEW_SCOTLAND_RADIO_STAGE)
                ),
                ImmutableSet.of(YOUVIEW_SCOTLAND_RADIO)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(YOUVIEW_SCOTLAND_RADIO)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                YOUVIEW_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeYouviewScotlandRadioStageConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                Sets.difference(
                        TARGET_SOURCES,
                        ImmutableSet.of(YOUVIEW_SCOTLAND_RADIO)
                ),
                ImmutableSet.of(YOUVIEW_SCOTLAND_RADIO_STAGE)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(YOUVIEW_SCOTLAND_RADIO_STAGE)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                YOUVIEW_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBbcReduxConfiguration() {
        ImmutableSet<Publisher> targetSources = MoreSets.add(
                TARGET_SOURCES,
                BBC_REDUX
        );

        return UpdaterConfiguration.builder()
                .withSource(BBC_REDUX)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM, targetSources
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeC4PressConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(C4_PRESS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BROADCAST_ITEM, ImmutableSet.of(PA)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBettyConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BETTY)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BETTY_ITEM, ImmutableSet.of(BETTY, YOUVIEW)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeFacebookConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(FACEBOOK)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_ITEM, ImmutableSet.of()
                        ),
                        NOP_ITEM_HANDLER,
                        NOP_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                FACEBOOK_CONTAINER, Sets.union(
                                        TARGET_SOURCES,
                                        ImmutableSet.of(FACEBOOK)
                                )
                                        .immutableCopy()
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeITunesConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(ITUNES)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_ITEM, TARGET_SOURCES
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_CONTAINER, TARGET_SOURCES
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeLovefilmConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                TARGET_SOURCES,
                ImmutableSet.of(LOVEFILM)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(LOVEFILM)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_WITH_SERIES_SEQUENCE_ITEM, targetSources
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeNetflixConfiguration() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(BBC, NETFLIX);

        return UpdaterConfiguration.builder()
                .withSource(NETFLIX)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_ITEM, targetSources
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeAmazonUnboxConfiguration() {
        ImmutableSet<Publisher> amazonSource =
                ImmutableSet.of(AMAZON_UNBOX);
        ImmutableSet<Publisher> otherSources =
                ImmutableSet.of(PA, RADIO_TIMES_UPCOMING);

        return UpdaterConfiguration.builder()
                .withSource(AMAZON_UNBOX)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                AMAZON_AMAZON_ITEM, amazonSource,
                                AMAZON_ITEM, otherSources,
                                IMDB_ITEM, ImmutableSet.of(IMDB, JUSTWATCH),
                                ITEM_SEQUENCE, ImmutableSet.of(IMDB, JUSTWATCH),
                                ITEM_SEARCH, ImmutableSet.of(IMDB, JUSTWATCH)
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                AMAZON_AMAZON_CONTAINER, amazonSource,
                                AMAZON_CONTAINER, otherSources,
                                IMDB_CONTAINER, ImmutableSet.of(IMDB, JUSTWATCH)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                AMAZON_AMAZON_SERIES, amazonSource,
                                AMAZON_SERIES, otherSources,
                                IMDB_SERIES, ImmutableSet.of(IMDB, JUSTWATCH)
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeTalkTalkConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(TALK_TALK)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_ITEM, TARGET_SOURCES
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_CONTAINER, TARGET_SOURCES
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBtVodConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BT_VOD)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_WITH_SERIES_SEQUENCE_ITEM, ImmutableSet.of(PA)
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                VOD_CONTAINER, ImmutableSet.of(PA)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of(PA)
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBtTveVodConfiguration() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(PA);

        return UpdaterConfiguration.builder()
                .withSource(BT_TVE_VOD)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BT_VOD_ITEM, targetSources
                        ),
                        STRICT_EPISODE_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                BT_VOD_CONTAINER, targetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, targetSources
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static ImmutableList<UpdaterConfiguration> makeVfConfigurations() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(PA);

        return VF_SOURCES.stream()
                .map(source -> UpdaterConfiguration.builder()
                        .withSource(source)
                        .withItemEquivalenceUpdater(
                                ImmutableMap.of(
                                        BT_VOD_ITEM, targetSources
                                ),
                                STRICT_EPISODE_ITEM_HANDLER,
                                STANDARD_ITEM_MESSENGER
                        )
                        .withTopLevelContainerEquivalenceUpdater(
                                ImmutableMap.of(
                                        BT_VOD_CONTAINER, targetSources
                                ),
                                STANDARD_CONTAINER_HANDLER,
                                STANDARD_CONTAINER_MESSENGER
                        )
                        .withNonTopLevelContainerEquivalenceUpdater(
                                ImmutableMap.of(
                                        STANDARD_SERIES, targetSources
                                ),
                                STANDARD_SERIES_HANDLER,
                                STANDARD_SERIES_MESSENGER
                        )
                        .build())
                .collect(MoreCollectors.toImmutableList());
    }

    private static UpdaterConfiguration makeRteConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(RTE)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_ITEM, ImmutableSet.of()
                        ),
                        NOP_ITEM_HANDLER,
                        NOP_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                RTE_VOD_CONTAINER, ImmutableSet.of(PA)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeFiveConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(FIVE)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(BBC_NITRO, ITV_CPS, UKTV, C4_PMLSD)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, TARGET_SOURCES
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBarbMasterConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BARB_MASTER)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(
                                        BBC_NITRO,
                                        ITV_CPS,
                                        UKTV,
                                        C4_PMLSD,
                                        C5_DATA_SUBMISSION,
                                        BARB_MASTER,
                                        BARB_TRANSMISSIONS,
                                        BARB_X_MASTER,
                                        BARB_CENSUS,
                                        BARB_NLE
                                )
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of()
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of()
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBarbTransmissionConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BARB_TRANSMISSIONS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                TXLOGS_ITEM, ImmutableSet.of(
                                        ITV_CPS,
                                        UKTV,
                                        C4_PMLSD,
                                        C5_DATA_SUBMISSION,
                                        BARB_TRANSMISSIONS,
                                        BARB_MASTER
                                ),
                                TXLOGS_TO_BBC_ITEM, ImmutableSet.of(BBC_NITRO)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of()
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of()
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeLayer3TxlogsConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(LAYER3_TXLOGS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                TXLOGS_TO_BBC_ITEM, ImmutableSet.of(BBC_NITRO),
                                BBC_REGIONAL_TXLOGS_ITEM, ImmutableSet.of(LAYER3_TXLOGS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeAeConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(AE_NETWORKS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                AE_TO_TXLOGS_ITEM, ImmutableSet.of(BARB_TRANSMISSIONS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeItvCpsConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(ITV_CPS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(PA, BBC_NITRO, UKTV, BARB_MASTER),
                                TXLOGS_ITEM, ImmutableSet.of(BARB_TRANSMISSIONS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                //nitro and uktv were removed since they are explicitly equived to by editors
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of(PA)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of(PA, BBC_NITRO, UKTV)
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeNitroConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BBC_NITRO)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_ITEM, ImmutableSet.of(PA, UKTV),
                                BARB_ITEM, ImmutableSet.of(BARB_MASTER),
                                BBC_TO_TXLOGS_ITEM, ImmutableSet.of(BARB_TRANSMISSIONS, LAYER3_TXLOGS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                //uktv was removed since it is explicitly equived to by editors
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of(PA)
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of(PA, UKTV)
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeC4PmlsdConfiguration() {
        ImmutableSet<Publisher> topLevelContainerTargetSources =
                Sets.difference(
                        TARGET_SOURCES,
                        //Top level containers from the following sources are explicitly equived to by editors
                        ImmutableSet.of(BBC_NITRO, ITV_CPS, C4_PMLSD, C5_DATA_SUBMISSION, UKTV)
                ).immutableCopy();
        return UpdaterConfiguration.builder()
                .withSource(C4_PMLSD)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_ITEM, ImmutableSet.of(PA, RADIO_TIMES),
                                BARB_ITEM, ImmutableSet.of(BARB_MASTER),
                                TXLOGS_ITEM, ImmutableSet.of(BARB_TRANSMISSIONS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, topLevelContainerTargetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeUktvConfiguration() {
        ImmutableSet<Publisher> topLevelContainerTargetSources =
                Sets.difference(
                        TARGET_SOURCES,
                        //Top level containers from the following sources are explicitly equived to by editors
                        ImmutableSet.of(BBC_NITRO, ITV_CPS, C4_PMLSD, C5_DATA_SUBMISSION, UKTV)
                ).immutableCopy();
        return UpdaterConfiguration.builder()
                .withSource(UKTV)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(PA, RADIO_TIMES, BARB_MASTER),
                                TXLOGS_ITEM, ImmutableSet.of(BARB_TRANSMISSIONS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, topLevelContainerTargetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeC5DataSubmissionConfiguration() {
        ImmutableSet<Publisher> topLevelContainerTargetSources =
                Sets.difference(
                        TARGET_SOURCES,
                        //Top level containers from the following sources are explicitly equived to by editors
                        ImmutableSet.of(BBC_NITRO, ITV_CPS, C4_PMLSD, C5_DATA_SUBMISSION, UKTV)
                ).immutableCopy();
        return UpdaterConfiguration.builder()
                .withSource(C5_DATA_SUBMISSION)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(PA, RADIO_TIMES, BARB_MASTER),
                                TXLOGS_ITEM, ImmutableSet.of(BARB_TRANSMISSIONS)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, topLevelContainerTargetSources
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, TARGET_SOURCES
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeViacom18DataSubmissionConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(VIACOM_18_DATA_SUBMISSION)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_ITEM, ImmutableSet.of()
                        ),
                        NOP_ITEM_HANDLER,
                        NOP_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeVimnDataSubmissionConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(VIMN_DATA_SUBMISSION)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_ITEM, ImmutableSet.of()
                        ),
                        NOP_ITEM_HANDLER,
                        NOP_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                NOP_CONTAINER, ImmutableSet.of()
                        ),
                        NOP_CONTAINER_HANDLER,
                        NOP_CONTAINER_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBarbCensusConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BARB_CENSUS)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(BARB_MASTER)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of()
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_SERIES, ImmutableSet.of()
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static UpdaterConfiguration makeBarbNleConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BARB_NLE)
                .withItemEquivalenceUpdater(
                        ImmutableMap.of(
                                BARB_ITEM, ImmutableSet.of(BARB_MASTER)
                        ),
                        STANDARD_ITEM_HANDLER,
                        STANDARD_ITEM_MESSENGER
                )
                .withTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                                STANDARD_TOP_LEVEL_CONTAINER, ImmutableSet.of()
                        ),
                        STANDARD_CONTAINER_HANDLER,
                        STANDARD_CONTAINER_MESSENGER
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        ImmutableMap.of(
                            STANDARD_SERIES, ImmutableSet.of()
                        ),
                        STANDARD_SERIES_HANDLER,
                        STANDARD_SERIES_MESSENGER
                )
                .build();
    }

    private static ImmutableList<UpdaterConfiguration> makeRoviConfigurations() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(
                Publisher.BBC,
                Publisher.PA,
                Publisher.YOUVIEW,
                Publisher.BBC_NITRO,
                Publisher.BBC_REDUX,
                Publisher.ITV,
                Publisher.C4_PMLSD,
                Publisher.C4_PMLSD_P06,
                Publisher.FIVE
        );

        return ImmutableList.of(ROVI_EN_GB, ROVI_EN_US)
                .stream()
                .map(source -> UpdaterConfiguration.builder()
                        .withSource(source)
                        .withItemEquivalenceUpdater(
                                ImmutableMap.of(
                                        ROVI_ITEM, targetSources
                                ),
                                STRICT_EPISODE_ITEM_HANDLER,
                                STANDARD_ITEM_MESSENGER
                        )
                        .withTopLevelContainerEquivalenceUpdater(
                                ImmutableMap.of(
                                        STANDARD_TOP_LEVEL_CONTAINER, targetSources
                                ),
                                STANDARD_CONTAINER_HANDLER,
                                STANDARD_CONTAINER_MESSENGER
                        )
                        .withNonTopLevelContainerEquivalenceUpdater(
                                ImmutableMap.of(
                                        NOP_CONTAINER, ImmutableSet.of()
                                ),
                                NOP_CONTAINER_HANDLER,
                                NOP_CONTAINER_MESSENGER
                        )
                        .build())
                .collect(MoreCollectors.toImmutableList());
    }

    private static ImmutableList<UpdaterConfiguration> makeMusicConfigurations() {
        return MUSIC_SOURCES.stream()
                .map(source -> UpdaterConfiguration.builder()
                        .withSource(source)
                        .withItemEquivalenceUpdater(
                                ImmutableMap.of(
                                        MUSIC_ITEM, Sets.union(
                                                MUSIC_SOURCES,
                                                ImmutableSet.of(ITUNES)
                                        )
                                                .immutableCopy()
                                ),
                                STANDARD_ITEM_HANDLER,
                                STANDARD_ITEM_MESSENGER
                        )
                        .withTopLevelContainerEquivalenceUpdater(
                                ImmutableMap.of(
                                        NOP_CONTAINER, ImmutableSet.of()
                                ),
                                NOP_CONTAINER_HANDLER,
                                NOP_CONTAINER_MESSENGER
                        )
                        .withNonTopLevelContainerEquivalenceUpdater(
                                ImmutableMap.of(
                                        NOP_CONTAINER, ImmutableSet.of()
                                ),
                                NOP_CONTAINER_HANDLER,
                                NOP_CONTAINER_MESSENGER
                        )
                        .build())
                .collect(MoreCollectors.toImmutableList());
    }
}
