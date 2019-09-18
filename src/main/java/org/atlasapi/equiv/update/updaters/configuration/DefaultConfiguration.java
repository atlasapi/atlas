package org.atlasapi.equiv.update.updaters.configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.atlasapi.media.entity.Publisher;

import static org.atlasapi.media.entity.Publisher.ADAPT_BBC_PODCASTS;
import static org.atlasapi.media.entity.Publisher.AMAZON_UK;
import static org.atlasapi.media.entity.Publisher.AMAZON_UNBOX;
import static org.atlasapi.media.entity.Publisher.AMC_EBS;
import static org.atlasapi.media.entity.Publisher.ARCHIVE_ORG;
import static org.atlasapi.media.entity.Publisher.ARQIVA;
import static org.atlasapi.media.entity.Publisher.BARB_CENSUS;
import static org.atlasapi.media.entity.Publisher.BARB_EDITOR_OVERRIDES;
import static org.atlasapi.media.entity.Publisher.BARB_MASTER;
import static org.atlasapi.media.entity.Publisher.BARB_NLE;
import static org.atlasapi.media.entity.Publisher.BARB_OVERRIDES;
import static org.atlasapi.media.entity.Publisher.BARB_TRANSMISSIONS;
import static org.atlasapi.media.entity.Publisher.BARB_X_MASTER;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_AUDIENCE_STATS;
import static org.atlasapi.media.entity.Publisher.BBC_KIWI;
import static org.atlasapi.media.entity.Publisher.BBC_LYREBIRD;
import static org.atlasapi.media.entity.Publisher.BBC_MUSIC;
import static org.atlasapi.media.entity.Publisher.BBC_NITRO;
import static org.atlasapi.media.entity.Publisher.BBC_PRODUCTS;
import static org.atlasapi.media.entity.Publisher.BBC_RD_TOPIC;
import static org.atlasapi.media.entity.Publisher.BBC_REDUX;
import static org.atlasapi.media.entity.Publisher.BETTY;
import static org.atlasapi.media.entity.Publisher.BLIP;
import static org.atlasapi.media.entity.Publisher.BT;
import static org.atlasapi.media.entity.Publisher.BT_BLACKOUT;
import static org.atlasapi.media.entity.Publisher.BT_EVENTS;
import static org.atlasapi.media.entity.Publisher.BT_FEATURED_CONTENT;
import static org.atlasapi.media.entity.Publisher.BT_SPORT_DANTE;
import static org.atlasapi.media.entity.Publisher.BT_SPORT_EBS;
import static org.atlasapi.media.entity.Publisher.BT_SPORT_ZEUS;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_PROD_CONFIG2;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_SYSTEST2_CONFIG_1;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_SYSTEST2_CONFIG_2;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_VOLD_CONFIG_1;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_VOLD_CONFIG_2;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_VOLE_CONFIG_1;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_VOLE_CONFIG_2;
import static org.atlasapi.media.entity.Publisher.BT_TV_CHANNELS;
import static org.atlasapi.media.entity.Publisher.BT_TV_CHANNELS_REFERENCE;
import static org.atlasapi.media.entity.Publisher.BT_TV_CHANNELS_TEST1;
import static org.atlasapi.media.entity.Publisher.BT_TV_CHANNELS_TEST2;
import static org.atlasapi.media.entity.Publisher.BT_TV_CHANNEL_GROUPS;
import static org.atlasapi.media.entity.Publisher.BT_VOD;
import static org.atlasapi.media.entity.Publisher.C4;
import static org.atlasapi.media.entity.Publisher.C4_INT;
import static org.atlasapi.media.entity.Publisher.C4_PMLSD;
import static org.atlasapi.media.entity.Publisher.C4_PMLSD_P06;
import static org.atlasapi.media.entity.Publisher.C4_PRESS;
import static org.atlasapi.media.entity.Publisher.C5_DATA_SUBMISSION;
import static org.atlasapi.media.entity.Publisher.C5_TV_CLIPS;
import static org.atlasapi.media.entity.Publisher.CANARY;
import static org.atlasapi.media.entity.Publisher.COYOTE;
import static org.atlasapi.media.entity.Publisher.DAILYMOTION;
import static org.atlasapi.media.entity.Publisher.DBPEDIA;
import static org.atlasapi.media.entity.Publisher.DIGITALSPY_RELATED_LINKS;
import static org.atlasapi.media.entity.Publisher.DOTMEDIA;
import static org.atlasapi.media.entity.Publisher.EBMS_VF_UK;
import static org.atlasapi.media.entity.Publisher.EMI_MUSIC;
import static org.atlasapi.media.entity.Publisher.EMI_PUB;
import static org.atlasapi.media.entity.Publisher.EVENT_MATCHER;
import static org.atlasapi.media.entity.Publisher.FACEBOOK;
import static org.atlasapi.media.entity.Publisher.FIVE;
import static org.atlasapi.media.entity.Publisher.FLICKR;
import static org.atlasapi.media.entity.Publisher.HBO;
import static org.atlasapi.media.entity.Publisher.HULU;
import static org.atlasapi.media.entity.Publisher.ICTOMORROW;
import static org.atlasapi.media.entity.Publisher.IMDB_API;
import static org.atlasapi.media.entity.Publisher.INTERNET_VIDEO_ARCHIVE;
import static org.atlasapi.media.entity.Publisher.ITUNES;
import static org.atlasapi.media.entity.Publisher.ITV;
import static org.atlasapi.media.entity.Publisher.ITV_CPS;
import static org.atlasapi.media.entity.Publisher.ITV_INTERLINKING;
import static org.atlasapi.media.entity.Publisher.KANDL_TOPICS;
import static org.atlasapi.media.entity.Publisher.KM_AP;
import static org.atlasapi.media.entity.Publisher.KM_BBC_WORLDWIDE;
import static org.atlasapi.media.entity.Publisher.KM_BLOOMBERG;
import static org.atlasapi.media.entity.Publisher.KM_GETTY;
import static org.atlasapi.media.entity.Publisher.KM_GLOBALIMAGEWORKS;
import static org.atlasapi.media.entity.Publisher.KM_MOVIETONE;
import static org.atlasapi.media.entity.Publisher.LAYER3_TXLOGS;
import static org.atlasapi.media.entity.Publisher.LONDON_ALSO;
import static org.atlasapi.media.entity.Publisher.LOVEFILM;
import static org.atlasapi.media.entity.Publisher.LYREBIRD_YOUTUBE;
import static org.atlasapi.media.entity.Publisher.MAGPIE;
import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.atlasapi.media.entity.Publisher.METABROADCAST_PICKS;
import static org.atlasapi.media.entity.Publisher.METABROADCAST_SIMILAR_CONTENT;
import static org.atlasapi.media.entity.Publisher.MSN_VIDEO;
import static org.atlasapi.media.entity.Publisher.MUSIC_BRAINZ;
import static org.atlasapi.media.entity.Publisher.NETFLIX;
import static org.atlasapi.media.entity.Publisher.NONAME_TV;
import static org.atlasapi.media.entity.Publisher.OPTA;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.atlasapi.media.entity.Publisher.PA_FEATURES;
import static org.atlasapi.media.entity.Publisher.PA_FEATURES_IRELAND;
import static org.atlasapi.media.entity.Publisher.PA_FEATURES_SOAP_ENTERTAINMENT;
import static org.atlasapi.media.entity.Publisher.PA_PEOPLE;
import static org.atlasapi.media.entity.Publisher.PA_SERIES_SUMMARIES;
import static org.atlasapi.media.entity.Publisher.PRIORITIZER;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES_UPCOMING;
import static org.atlasapi.media.entity.Publisher.RDIO;
import static org.atlasapi.media.entity.Publisher.REDBEE_BDS;
import static org.atlasapi.media.entity.Publisher.REDBEE_MEDIA;
import static org.atlasapi.media.entity.Publisher.RTE;
import static org.atlasapi.media.entity.Publisher.SCRAPERWIKI;
import static org.atlasapi.media.entity.Publisher.SCRUBBABLES;
import static org.atlasapi.media.entity.Publisher.SCRUBBABLES_PRODUCER;
import static org.atlasapi.media.entity.Publisher.SEESAW;
import static org.atlasapi.media.entity.Publisher.SOUNDCLOUD;
import static org.atlasapi.media.entity.Publisher.SPOTIFY;
import static org.atlasapi.media.entity.Publisher.SVERIGES_RADIO;
import static org.atlasapi.media.entity.Publisher.TALK_TALK;
import static org.atlasapi.media.entity.Publisher.TED;
import static org.atlasapi.media.entity.Publisher.THESPACE;
import static org.atlasapi.media.entity.Publisher.THETVDB;
import static org.atlasapi.media.entity.Publisher.THE_SUN;
import static org.atlasapi.media.entity.Publisher.TMS_EN_GB;
import static org.atlasapi.media.entity.Publisher.TVBLOB;
import static org.atlasapi.media.entity.Publisher.TVCHOICE_RELATED_LINKS;
import static org.atlasapi.media.entity.Publisher.TWITCH;
import static org.atlasapi.media.entity.Publisher.UKTV;
import static org.atlasapi.media.entity.Publisher.VF_AE;
import static org.atlasapi.media.entity.Publisher.VF_BBC;
import static org.atlasapi.media.entity.Publisher.VF_C5;
import static org.atlasapi.media.entity.Publisher.VF_ITV;
import static org.atlasapi.media.entity.Publisher.VF_OVERRIDES;
import static org.atlasapi.media.entity.Publisher.VF_VIACOM;
import static org.atlasapi.media.entity.Publisher.VF_VUBIQUITY;
import static org.atlasapi.media.entity.Publisher.VIMEO;
import static org.atlasapi.media.entity.Publisher.VOILA;
import static org.atlasapi.media.entity.Publisher.VOILA_CHANNEL_GROUPS;
import static org.atlasapi.media.entity.Publisher.WIKIPEDIA;
import static org.atlasapi.media.entity.Publisher.WORLD_SERVICE;
import static org.atlasapi.media.entity.Publisher.YOUTUBE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_STAGE;
import static org.atlasapi.media.entity.Publisher.YURI;

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
                            C5_DATA_SUBMISSION,
                            BARB_CENSUS,
                            BARB_NLE,
                            BARB_EDITOR_OVERRIDES
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
