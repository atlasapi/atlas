package org.atlasapi.equiv.update.updaters.configuration;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.MUSIC_SOURCES;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration
        .NON_STANDARD_SOURCES;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.TARGET_SOURCES;
import static org.atlasapi.equiv.update.updaters.configuration.DefaultConfiguration.VF_SOURCES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .BROADCAST_ITEM_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .BT_VOD_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .FACEBOOK_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .NOP_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .RTE_VOD_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .RT_UPCOMING_TOP_LEVEL_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .STANDARD_SERIES;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .STANDARD_TOP_LEVEL_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType
        .VOD_CONTAINER;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.BETTY_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.BROADCAST_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.BT_VOD_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.EBS_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.FIVE_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.MUSIC_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.NOP_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.ROVI_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.RT_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.RT_UPCOMING_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.STANDARD_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.VOD_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType
        .VOD_WITH_SERIES_SEQUENCE_ITEM;
import static org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType.YOUVIEW_ITEM;
import static org.atlasapi.media.entity.Publisher.AMAZON_UNBOX;
import static org.atlasapi.media.entity.Publisher.AMC_EBS;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_REDUX;
import static org.atlasapi.media.entity.Publisher.BETTY;
import static org.atlasapi.media.entity.Publisher.BT_SPORT_EBS;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD;
import static org.atlasapi.media.entity.Publisher.BT_VOD;
import static org.atlasapi.media.entity.Publisher.C4_PRESS;
import static org.atlasapi.media.entity.Publisher.FACEBOOK;
import static org.atlasapi.media.entity.Publisher.FIVE;
import static org.atlasapi.media.entity.Publisher.ITUNES;
import static org.atlasapi.media.entity.Publisher.LOVEFILM;
import static org.atlasapi.media.entity.Publisher.NETFLIX;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.atlasapi.media.entity.Publisher.PREVIEW_NETWORKS;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES_UPCOMING;
import static org.atlasapi.media.entity.Publisher.ROVI_EN_GB;
import static org.atlasapi.media.entity.Publisher.ROVI_EN_US;
import static org.atlasapi.media.entity.Publisher.RTE;
import static org.atlasapi.media.entity.Publisher.TALK_TALK;
import static org.atlasapi.media.entity.Publisher.YOUVIEW;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_STAGE;

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

    private ImmutableList<UpdaterConfiguration> makeConfigurations() {
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
                makeFiveConfiguration()
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

        return configurations.build();
    }

    private ImmutableList<UpdaterConfiguration> makeDefaultConfigurations() {
        return Publisher.all()
                .stream()
                .filter(source -> !NON_STANDARD_SOURCES.contains(source))
                .map(this::makeDefaultConfiguration)
                .collect(MoreCollectors.toImmutableList());
    }

    private UpdaterConfiguration makeDefaultConfiguration(Publisher publisher) {
        return UpdaterConfiguration.builder()
                .withSource(publisher)
                .withItemEquivalenceUpdater(
                        STANDARD_ITEM,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        STANDARD_TOP_LEVEL_CONTAINER,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        TARGET_SOURCES
                )
                .build();
    }

    private UpdaterConfiguration makeRadioTimesUpcomingConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(RADIO_TIMES_UPCOMING)
                .withItemEquivalenceUpdater(
                        RT_UPCOMING_ITEM,
                        ImmutableSet.of(AMAZON_UNBOX)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        RT_UPCOMING_TOP_LEVEL_CONTAINER,
                        ImmutableSet.of(AMAZON_UNBOX)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        ImmutableSet.of(AMAZON_UNBOX)
                )
                .build();
    }

    private UpdaterConfiguration makeEbsConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BT_SPORT_EBS)
                .withItemEquivalenceUpdater(
                        EBS_ITEM,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        STANDARD_TOP_LEVEL_CONTAINER,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        TARGET_SOURCES
                )
                .build();
    }

    private UpdaterConfiguration makeAmcEbsConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(AMC_EBS)
                .withItemEquivalenceUpdater(
                        STANDARD_ITEM,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        STANDARD_TOP_LEVEL_CONTAINER,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        ImmutableSet.of(AMC_EBS, PA)
                )
                .build();
    }

    private UpdaterConfiguration makeRadioTimesConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(RADIO_TIMES)
                .withItemEquivalenceUpdater(
                        RT_ITEM,
                        ImmutableSet.of(PREVIEW_NETWORKS)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeYouviewConfiguration() {
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
                        YOUVIEW_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeYouviewStageConfiguration() {
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
                        YOUVIEW_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeYouviewBtConfiguration() {
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
                        YOUVIEW_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeYouviewBtStageConfiguration() {
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
                        YOUVIEW_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeYouviewScotlandRadioConfiguration() {
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
                        YOUVIEW_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeYouviewScotlandRadioStageConfiguration() {
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
                        YOUVIEW_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeBbcReduxConfiguration() {
        ImmutableSet<Publisher> targetSources = MoreSets.add(
                TARGET_SOURCES,
                BBC_REDUX
        );

        return UpdaterConfiguration.builder()
                .withSource(BBC_REDUX)
                .withItemEquivalenceUpdater(
                        BROADCAST_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BROADCAST_ITEM_CONTAINER,
                        targetSources

                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeC4PressConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(C4_PRESS)
                .withItemEquivalenceUpdater(
                        BROADCAST_ITEM,
                        ImmutableSet.of(PA)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeBettyConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BETTY)
                .withItemEquivalenceUpdater(
                        BETTY_ITEM,
                        ImmutableSet.of(BETTY, YOUVIEW)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeFacebookConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(FACEBOOK)
                .withItemEquivalenceUpdater(
                        NOP_ITEM,
                        ImmutableSet.of()
                )
                .withTopLevelContainerEquivalenceUpdater(
                        FACEBOOK_CONTAINER,
                        Sets.union(
                                TARGET_SOURCES,
                                ImmutableSet.of(FACEBOOK)
                        )
                                .immutableCopy()
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeITunesConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(ITUNES)
                .withItemEquivalenceUpdater(
                        VOD_ITEM,
                        TARGET_SOURCES
                )
                .withTopLevelContainerEquivalenceUpdater(
                        VOD_CONTAINER,
                        TARGET_SOURCES
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeLovefilmConfiguration() {
        ImmutableSet<Publisher> targetSources = Sets.union(
                TARGET_SOURCES,
                ImmutableSet.of(LOVEFILM)
        )
                .immutableCopy();

        return UpdaterConfiguration.builder()
                .withSource(LOVEFILM)
                .withItemEquivalenceUpdater(
                        VOD_WITH_SERIES_SEQUENCE_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        VOD_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeNetflixConfiguration() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(BBC, NETFLIX);

        return UpdaterConfiguration.builder()
                .withSource(NETFLIX)
                .withItemEquivalenceUpdater(
                        VOD_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        VOD_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeAmazonUnboxConfiguration() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(AMAZON_UNBOX, PA);

        return UpdaterConfiguration.builder()
                .withSource(AMAZON_UNBOX)
                .withItemEquivalenceUpdater(
                        VOD_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        VOD_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeTalkTalkConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(TALK_TALK)
                .withItemEquivalenceUpdater(
                        VOD_ITEM,
                        TARGET_SOURCES
                )
                .withTopLevelContainerEquivalenceUpdater(
                        VOD_CONTAINER,
                        TARGET_SOURCES
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        TARGET_SOURCES
                )
                .build();
    }

    private UpdaterConfiguration makeBtVodConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(BT_VOD)
                .withItemEquivalenceUpdater(
                        VOD_WITH_SERIES_SEQUENCE_ITEM,
                        ImmutableSet.of(PA)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        VOD_CONTAINER,
                        ImmutableSet.of(PA)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        ImmutableSet.of(PA)
                )
                .build();
    }

    private UpdaterConfiguration makeBtTveVodConfiguration() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(PA);

        return UpdaterConfiguration.builder()
                .withSource(BT_TVE_VOD)
                .withItemEquivalenceUpdater(
                        BT_VOD_ITEM,
                        targetSources
                )
                .withTopLevelContainerEquivalenceUpdater(
                        BT_VOD_CONTAINER,
                        targetSources
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        targetSources
                )
                .build();
    }

    private ImmutableList<UpdaterConfiguration> makeVfConfigurations() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(PA);

        return VF_SOURCES.stream()
                .map(source -> UpdaterConfiguration.builder()
                        .withSource(source)
                        .withItemEquivalenceUpdater(
                                BT_VOD_ITEM,
                                targetSources
                        )
                        .withTopLevelContainerEquivalenceUpdater(
                                BT_VOD_CONTAINER,
                                targetSources
                        )
                        .withNonTopLevelContainerEquivalenceUpdater(
                                STANDARD_SERIES,
                                targetSources
                        )
                        .build())
                .collect(MoreCollectors.toImmutableList());
    }

    private UpdaterConfiguration makeRteConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(RTE)
                .withItemEquivalenceUpdater(
                        NOP_ITEM,
                        ImmutableSet.of()
                )
                .withTopLevelContainerEquivalenceUpdater(
                        RTE_VOD_CONTAINER,
                        ImmutableSet.of(PA)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        NOP_CONTAINER,
                        ImmutableSet.of()
                )
                .build();
    }

    private UpdaterConfiguration makeFiveConfiguration() {
        return UpdaterConfiguration.builder()
                .withSource(FIVE)
                .withItemEquivalenceUpdater(
                        FIVE_ITEM,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withTopLevelContainerEquivalenceUpdater(
                        STANDARD_TOP_LEVEL_CONTAINER,
                        MoreSets.add(TARGET_SOURCES, LOVEFILM)
                )
                .withNonTopLevelContainerEquivalenceUpdater(
                        STANDARD_SERIES,
                        TARGET_SOURCES
                )
                .build();
    }

    private ImmutableList<UpdaterConfiguration> makeRoviConfigurations() {
        ImmutableSet<Publisher> targetSources = ImmutableSet.of(
                Publisher.BBC,
                Publisher.PA,
                Publisher.YOUVIEW,
                Publisher.BBC_NITRO,
                Publisher.BBC_REDUX,
                Publisher.ITV,
                Publisher.C4_PMLSD,
                Publisher.C4_PMLSD_P06,
                FIVE
        );

        return ImmutableList.of(ROVI_EN_GB, ROVI_EN_US)
                .stream()
                .map(source -> UpdaterConfiguration.builder()
                        .withSource(source)
                        .withItemEquivalenceUpdater(
                                ROVI_ITEM,
                                targetSources
                        )
                        .withTopLevelContainerEquivalenceUpdater(
                                STANDARD_TOP_LEVEL_CONTAINER,
                                targetSources
                        )
                        .withNonTopLevelContainerEquivalenceUpdater(
                                NOP_CONTAINER,
                                ImmutableSet.of()
                        )
                        .build())
                .collect(MoreCollectors.toImmutableList());
    }

    private ImmutableList<UpdaterConfiguration> makeMusicConfigurations() {
        return MUSIC_SOURCES.stream()
                .map(source -> UpdaterConfiguration.builder()
                        .withSource(source)
                        .withItemEquivalenceUpdater(
                                MUSIC_ITEM,
                                Sets.union(
                                        MUSIC_SOURCES,
                                        ImmutableSet.of(ITUNES)
                                )
                                        .immutableCopy()
                        )
                        .withTopLevelContainerEquivalenceUpdater(
                                NOP_CONTAINER,
                                ImmutableSet.of()
                        )
                        .withNonTopLevelContainerEquivalenceUpdater(
                                NOP_CONTAINER,
                                ImmutableSet.of()
                        )
                        .build())
                .collect(MoreCollectors.toImmutableList());
    }
}
