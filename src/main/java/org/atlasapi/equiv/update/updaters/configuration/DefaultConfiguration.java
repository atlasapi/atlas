package org.atlasapi.equiv.update.updaters.configuration;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import static org.atlasapi.media.entity.Publisher.AMAZON_UK;
import static org.atlasapi.media.entity.Publisher.AMAZON_UNBOX;
import static org.atlasapi.media.entity.Publisher.AMC_EBS;
import static org.atlasapi.media.entity.Publisher.BARB_MASTER;
import static org.atlasapi.media.entity.Publisher.BARB_TRANSMISSIONS;
import static org.atlasapi.media.entity.Publisher.BARB_X_MASTER;
import static org.atlasapi.media.entity.Publisher.BBC_MUSIC;
import static org.atlasapi.media.entity.Publisher.BBC_NITRO;
import static org.atlasapi.media.entity.Publisher.BBC_REDUX;
import static org.atlasapi.media.entity.Publisher.BETTY;
import static org.atlasapi.media.entity.Publisher.BT_SPORT_EBS;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD;
import static org.atlasapi.media.entity.Publisher.BT_VOD;
import static org.atlasapi.media.entity.Publisher.C4_PMLSD;
import static org.atlasapi.media.entity.Publisher.C4_PRESS;
import static org.atlasapi.media.entity.Publisher.FACEBOOK;
import static org.atlasapi.media.entity.Publisher.FIVE;
import static org.atlasapi.media.entity.Publisher.ITUNES;
import static org.atlasapi.media.entity.Publisher.ITV_CPS;
import static org.atlasapi.media.entity.Publisher.LOVEFILM;
import static org.atlasapi.media.entity.Publisher.NETFLIX;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.atlasapi.media.entity.Publisher.PA_SERIES_SUMMARIES;
import static org.atlasapi.media.entity.Publisher.PREVIEW_NETWORKS;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES_UPCOMING;
import static org.atlasapi.media.entity.Publisher.RDIO;
import static org.atlasapi.media.entity.Publisher.RTE;
import static org.atlasapi.media.entity.Publisher.SOUNDCLOUD;
import static org.atlasapi.media.entity.Publisher.SPOTIFY;
import static org.atlasapi.media.entity.Publisher.TALK_TALK;
import static org.atlasapi.media.entity.Publisher.UKTV;
import static org.atlasapi.media.entity.Publisher.VF_AE;
import static org.atlasapi.media.entity.Publisher.VF_BBC;
import static org.atlasapi.media.entity.Publisher.VF_C5;
import static org.atlasapi.media.entity.Publisher.VF_ITV;
import static org.atlasapi.media.entity.Publisher.VF_VIACOM;
import static org.atlasapi.media.entity.Publisher.VF_VUBIQUITY;
import static org.atlasapi.media.entity.Publisher.WIKIPEDIA;
import static org.atlasapi.media.entity.Publisher.YOUTUBE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_STAGE;

public class DefaultConfiguration {

    public static final ImmutableSet<Publisher> MUSIC_SOURCES = ImmutableSet.of(
            BBC_MUSIC,
            YOUTUBE,
            SPOTIFY,
            SOUNDCLOUD,
            RDIO,
            AMAZON_UK
    );

    @SuppressWarnings("WeakerAccess")
    public static final ImmutableSet<Publisher> ROVI_SOURCES = ImmutableSet.copyOf(
            Sets.filter(
                    Publisher.all(),
                    input -> input.key().endsWith("rovicorp.com")
            )
    );

    public static final ImmutableSet<Publisher> VF_SOURCES = ImmutableSet.of(
            VF_AE,
            VF_BBC,
            VF_C5,
            VF_ITV,
            VF_VIACOM,
            VF_VUBIQUITY
    );

    public static final ImmutableSet<Publisher> NON_STANDARD_SOURCES = ImmutableSet
            .<Publisher>builder()
            .addAll(
                    ImmutableSet.of(
                            ITUNES,
                            BBC_REDUX,
                            RADIO_TIMES,
                            FACEBOOK,
                            LOVEFILM,
                            NETFLIX,
                            RTE,
                            YOUVIEW,
                            YOUVIEW_STAGE,
                            YOUVIEW_BT,
                            YOUVIEW_BT_STAGE,
                            TALK_TALK,
                            PA,
                            PA_SERIES_SUMMARIES,
                            BT_VOD,
                            BT_TVE_VOD,
                            BETTY,
                            AMC_EBS,
                            BT_SPORT_EBS,
                            C4_PRESS,
                            RADIO_TIMES_UPCOMING,
                            FIVE,
                            AMAZON_UNBOX,
                            YOUVIEW_SCOTLAND_RADIO,
                            YOUVIEW_SCOTLAND_RADIO_STAGE,
                            BARB_MASTER,
                            BARB_TRANSMISSIONS,
                            ITV_CPS,
                            BBC_NITRO,
                            C4_PMLSD,
                            UKTV,
                            WIKIPEDIA,
                            BARB_X_MASTER
                    )
            )
            .addAll(MUSIC_SOURCES)
            .addAll(ROVI_SOURCES)
            .addAll(VF_SOURCES)
            .build();

    public static final ImmutableSet<Publisher> TARGET_SOURCES = ImmutableSet.copyOf(
            Sets.difference(
                    Publisher.all(),
                    Sets.union(
                            ImmutableSet.of(
                                    PREVIEW_NETWORKS,
                                    BBC_REDUX,
                                    RADIO_TIMES,
                                    LOVEFILM,
                                    NETFLIX,
                                    YOUVIEW,
                                    YOUVIEW_STAGE,
                                    YOUVIEW_BT,
                                    YOUVIEW_BT_STAGE,
                                    BT_SPORT_EBS,
                                    C4_PRESS,
                                    RADIO_TIMES_UPCOMING
                            ),
                            Sets.union(
                                    MUSIC_SOURCES,
                                    ROVI_SOURCES
                    )
            )
    ));

    private DefaultConfiguration() {
        // Private to defeat instantiation
    }
}
