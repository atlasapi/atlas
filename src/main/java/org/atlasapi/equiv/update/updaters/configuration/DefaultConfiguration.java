package org.atlasapi.equiv.update.updaters.configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.atlasapi.media.entity.Publisher;

import static org.atlasapi.media.entity.Publisher.*;

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
                            LAYER3_TXLOGS,
                            BARB_OVERRIDES,
                            ITV_CPS,
                            BBC_NITRO,
                            C4_PMLSD,
                            UKTV,
                            WIKIPEDIA,
                            BARB_X_MASTER,
                            IMDB_API,
                            IMDB,
                            C5_DATA_SUBMISSION,
                            BARB_CENSUS,
                            BARB_NLE,
                            BARB_EDITOR_OVERRIDES,
                            JUSTWATCH
                    )
            )
            .addAll(MUSIC_SOURCES)
            .addAll(ROVI_SOURCES)
            .addAll(VF_SOURCES)
            .build();

    // This was changed from being a Sets.difference of all publishers and a select set of publishers
    // to a hardcoded list of what that Sets.difference actually was.
    // As a result there may be sources in here that aren't really desired or used.
    // The change was to prevent new sources being added automatically being included in equivalence results
    // inadvertently.
    public static final ImmutableSet<Publisher> TARGET_SOURCES = ImmutableSet.<Publisher>builder()
            .addAll(
                    ImmutableSet.of(
                            BBC,
                            C4,
                            HULU,
                            TED,
                            VIMEO,
                            ITV,
                            BLIP,
                            DAILYMOTION,
                            FLICKR,
                            FIVE,
                            SEESAW,
                            TVBLOB,
                            ICTOMORROW,
                            HBO,
                            ITUNES,
                            MSN_VIDEO,
                            PA,
                            PA_SERIES_SUMMARIES,
                            ARCHIVE_ORG,
                            WORLD_SERVICE,
                            METABROADCAST,
                            DBPEDIA,
                            BBC_PRODUCTS,
                            MUSIC_BRAINZ,
                            EMI_PUB,
                            EMI_MUSIC,
                            BBC_KIWI,
                            CANARY,
                            THESPACE,
                            VOILA,
                            VOILA_CHANNEL_GROUPS,
                            MAGPIE,
                            LONDON_ALSO,
                            BBC_RD_TOPIC,
                            PA_FEATURES,
                            PA_PEOPLE,
                            BT,
                            BT_FEATURED_CONTENT,
                            BT_VOD,
                            BT_TVE_VOD,
                            BT_TVE_VOD_VOLD_CONFIG_1,
                            BT_TVE_VOD_VOLE_CONFIG_1,
                            BT_TVE_VOD_SYSTEST2_CONFIG_1,
                            BT_TVE_VOD_PROD_CONFIG2,
                            BT_TVE_VOD_VOLD_CONFIG_2,
                            BT_TVE_VOD_VOLE_CONFIG_2,
                            BT_TVE_VOD_SYSTEST2_CONFIG_2,
                            BT_TV_CHANNELS,
                            BT_TV_CHANNELS_TEST1,
                            BT_TV_CHANNELS_TEST2,
                            BT_TV_CHANNELS_REFERENCE,
                            BT_TV_CHANNEL_GROUPS,
                            YOUVIEW_SCOTLAND_RADIO,
                            YOUVIEW_SCOTLAND_RADIO_STAGE,
                            FACEBOOK,
                            SCRAPERWIKI,
                            SVERIGES_RADIO,
                            TALK_TALK,
                            KANDL_TOPICS,
                            THE_SUN,
                            ADAPT_BBC_PODCASTS,
                            YURI,
                            COYOTE,
                            BBC_NITRO,
                            AMAZON_UNBOX,
                            METABROADCAST_PICKS,
                            C4_PMLSD,
                            C4_PMLSD_P06,
                            WIKIPEDIA,
                            C5_TV_CLIPS,
                            METABROADCAST_SIMILAR_CONTENT,
                            BBC_LYREBIRD,
                            LYREBIRD_YOUTUBE,
                            PA_FEATURES_IRELAND,
                            PA_FEATURES_SOAP_ENTERTAINMENT,
                            RTE,
                            BBC_AUDIENCE_STATS,
                            KM_BBC_WORLDWIDE,
                            KM_BLOOMBERG,
                            KM_GLOBALIMAGEWORKS,
                            KM_MOVIETONE,
                            KM_AP,
                            KM_GETTY,
                            BT_EVENTS,
                            OPTA,
                            BETTY,
                            BT_BLACKOUT,
                            SCRUBBABLES,
                            SCRUBBABLES_PRODUCER,
                            NONAME_TV,
                            THETVDB,
                            ITV_INTERLINKING,
                            DOTMEDIA,
                            PRIORITIZER,
                            BT_SPORT_ZEUS,
                            BT_SPORT_DANTE,
                            UKTV,
                            REDBEE_MEDIA,
                            EVENT_MATCHER,
                            VF_BBC,
                            VF_ITV,
                            VF_AE,
                            VF_C5,
                            VF_VIACOM,
                            VF_VUBIQUITY,
                            TMS_EN_GB,
                            VF_OVERRIDES,
                            REDBEE_BDS,
                            EBMS_VF_UK,
                            ARQIVA,
                            TVCHOICE_RELATED_LINKS,
                            INTERNET_VIDEO_ARCHIVE,
                            DIGITALSPY_RELATED_LINKS,
                            AMC_EBS,
                            TWITCH,
                            ITV_CPS,
                            C4_INT,
                            C5_DATA_SUBMISSION,
                            IMDB_API
                    )
            )
            .build();

    private DefaultConfiguration() {
        // Private to defeat instantiation
    }
}
