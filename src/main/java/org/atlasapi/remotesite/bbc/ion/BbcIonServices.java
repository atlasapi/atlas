package org.atlasapi.remotesite.bbc.ion;

import org.atlasapi.feeds.radioplayer.RadioPlayerService;
import org.atlasapi.feeds.radioplayer.RadioPlayerServices;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

public class BbcIonServices {

    public static BiMap<String, String> tvServices = ImmutableBiMap.<String, String>builder()

            //        .put("bbc_one",  "http://www.bbc.co.uk/services/bbcone")
            .put("bbc_one_london", "http://www.bbc.co.uk/services/bbcone/london")
            .put("bbc_one_west", "http://www.bbc.co.uk/services/bbcone/west")
            .put("bbc_one_south_west", "http://www.bbc.co.uk/services/bbcone/south_west")
            .put("bbc_one_south", "http://www.bbc.co.uk/services/bbcone/south")
            .put("bbc_one_south_east", "http://www.bbc.co.uk/services/bbcone/south_east")
            .put("bbc_one_west_midlands", "http://www.bbc.co.uk/services/bbcone/west_midlands")
            .put("bbc_one_east_midlands", "http://www.bbc.co.uk/services/bbcone/east_midlands")
            .put("bbc_one_east", "http://www.bbc.co.uk/services/bbcone/east")
            .put("bbc_one_north_east", "http://www.bbc.co.uk/services/bbcone/north_east")
            .put("bbc_one_north_west", "http://www.bbc.co.uk/services/bbcone/north_west")
            .put("bbc_one_scotland", "http://www.bbc.co.uk/services/bbcone/scotland")
            .put("bbc_one_yorks", "http://www.bbc.co.uk/services/bbcone/yorkshire")
            .put("bbc_one_oxford", "http://www.bbc.co.uk/services/bbcone/oxford")
            .put("bbc_one_cambridge", "http://www.bbc.co.uk/services/bbcone/cambridge")
            .put("bbc_one_channel_islands", "http://www.bbc.co.uk/services/bbcone/channel_islands")
            .put("bbc_one_east_yorkshire", "http://www.bbc.co.uk/services/bbcone/east_yorkshire")
            .put("bbc_one_northern_ireland", "http://www.bbc.co.uk/services/bbcone/ni")
            .put("bbc_one_wales", "http://www.bbc.co.uk/services/bbcone/wales")
            .put("bbc_one_hd", "http://www.bbc.co.uk/services/bbcone/hd")
            .put(
                    "bbc_one_northern_ireland_hd",
                    "http://ref.atlasapi.org/channels/pressassociation.com/1770"
            )
            .put(
                    "bbc_one_scotland_hd",
                    "http://ref.atlasapi.org/channels/pressassociation.com/1776"
            )
            .put("bbc_one_wales_hd", "http://ref.atlasapi.org/channels/pressassociation.com/1778")
            //        .put("bbc_two", "http://www.bbc.co.uk/services/bbctwo")
            .put("bbc_two_hd", "http://ref.atlasapi.org/channels/pressassociation.com/1782")
            .put("bbc_two_england", "http://www.bbc.co.uk/services/bbctwo/england")
            //        .put("bbc_two_wales", "http://www.bbc.co.uk/services/bbctwo/wales_analogue")
            .put("bbc_two_wales_digital", "http://www.bbc.co.uk/services/bbctwo/wales")
            .put("bbc_two_wales_hd", "http://nitro.bbc.co.uk/services/bbc_two_wales_hd_233a_447e")
            //        .put("bbc_two_northern_ireland", "http://www.bbc.co.uk/services/bbctwo/ni_analogue")
            .put("bbc_two_northern_ireland_digital", "http://www.bbc.co.uk/services/bbctwo/ni")
            .put("bbc_two_northern_ireland_hd","http://nitro.bbc.co.uk/services/bbc_two_northern_ireland_hd_233a_447d")
            .put("bbc_two_scotland", "http://www.bbc.co.uk/services/bbctwo/scotland")
            .put("bbc_three", "http://www.bbc.co.uk/services/bbcthree")
            .put("bbc_three_hd", "http://ref.atlasapi.org/channels/pressassociation.com/1852")
            .put("bbc_four", "http://www.bbc.co.uk/services/bbcfour")
            .put("bbc_four_hd", "http://ref.atlasapi.org/channels/pressassociation.com/1851")
            .put("bbc_scotland", "http://ref.atlasapi.org/channels/pressassociation.com/2232")
            .put("bbc_scotland_hd", "http://ref.atlasapi.org/channels/pressassociation.com/2233")
            .put("bbc_alba", "http://ref.atlasapi.org/channels/bbcalba")
            .put("bbc_parliament", "http://www.bbc.co.uk/services/parliament")
            .put("bbc_news24", "http://www.bbc.co.uk/services/bbcnews")
            .put(
                    "bbc_news_channel_hd",
                    "http://ref.atlasapi.org/channels/pressassociation.com/1853"
            )
            //        .put("bbc_hd",          "http://www.bbc.co.uk/services/bbchd")
            .put("cbbc", "http://www.bbc.co.uk/services/cbbc")
            .put("cbbc_hd", "http://ref.atlasapi.org/channels/pressassociation.com/1849")
            .put("cbeebies", "http://www.bbc.co.uk/services/cbeebies")
            .put("cbeebies_hd", "http://ref.atlasapi.org/channels/pressassociation.com/1850")
            .build();

    public static BiMap<String, String> radioServices = ImmutableBiMap.<String, String>builder()
            .putAll(
                    Maps.transformValues(
                            Maps.uniqueIndex(
                                    RadioPlayerServices.services,
                                    RadioPlayerService::getIonId
                            ),
                            RadioPlayerService::getServiceUri
                    )
            )
            .put("bbc_radio_fourlw", "http://www.bbc.co.uk/services/radio4/lw")
            .build();

    public static BiMap<String, String> services = ImmutableBiMap.<String, String>builder().putAll(
            tvServices).putAll(radioServices).build();

    // Master brands that don't already form part of either tvServices or radioServices
    public static BiMap<String, String> masterBrands = ImmutableBiMap.<String, String>builder()
            .putAll(Maps.filterKeys(services, not(in(ImmutableSet.of("bbc_radio_four_extra")))))
            .put("bbc_7", "http://www.bbc.co.uk/services/radio4extra")
            .put("bbc_news", "http://www.bbc.co.uk/services/bbc_news")
            .put("bbc_one", "http://ref.atlasapi.org/channels/pressassociation.com/stations/1")
            .put(
                    "bbc_radio_four",
                    "http://ref.atlasapi.org/channels/pressassociation.com/stations/5"
            )
            .put(
                    "bbc_radio_scotland",
                    "http://ref.atlasapi.org/channels/pressassociation.com/stations/153"
            )
            .put(
                    "bbc_radio_wales",
                    "http://ref.atlasapi.org/channels/pressassociation.com/stations/466"
            )
            .put("bbc_two", "http://ref.atlasapi.org/channels/pressassociation.com/stations/6")
            .put("bbc_sport", "http://www.bbc.co.uk/services/bbc_sport")
            .put("bbc_webonly", "http://www.bbc.co.uk/services/bbc_webonly")
            .put("bbc_music", "http://www.bbc.co.uk/services/bbc_music")
            .put("shakespeares_globe", "http://www.bbc.co.uk/services/shakespeares_globe")
            .put("british_film_institute", "http://www.bbc.co.uk/services/british_film_institute")
            .put("hay_festival", "http://www.bbc.co.uk/services/hay_festival")
            .put(
                    "royal_shakespeare_company",
                    "http://www.bbc.co.uk/services/royal_shakespeare_company"
            )
            .put("royal_opera_house", "http://www.bbc.co.uk/services/royal_opera_house")
            .put("british_council", "http://www.bbc.co.uk/services/british_council")
            .put(
                    "shakespeare_birthplace_trust",
                    "http://www.bbc.co.uk/services/shakespeare_birthplace_trust"
            )
            .put("ex_cathedra", "http://www.bbc.co.uk/services/ex_cathedra")
            .put(
                    "europeanbroadcastingunion",
                    "http://www.bbc.co.uk/services/europeanbroadcastingunion"
            )
            .put("bbc_arts", "http://www.bbc.co.uk/services/bbc_arts")
            //       I don't think the following are required, but leaving here so the full list can be
            //       checked
            //        .put("bbc_radio_swindon", "")
            //        .put("bbc_wales", "")
            //        .put("bbc_weather", "")
            //        .put("bbc_webonly", "")
                    .put("bbc_world_news", "http://www.bbc.co.uk/services/bbcworldnews")
            //        .put("bbc_radio_webonly", "")
            //        .put("bbc_school_radio", "")
            //        .put("bbc_southern_counties_radio", "")
            //        .put("bbc_sport", "")
            //        .put("bbc_switch", "")
            //        .put("bbc_cymru", "")
            //        .put("bbc_democracy_live", "")
            //        .put("bbc_local_radio", "")
            //        .put("bbc_music", "")
            .build();

    public static String get(String ionService) {
        return services.get(ionService);
    }

    public static String getMasterBrand(String ionService) {
        return masterBrands.get(ionService);
    }

    public static String reverseGet(String bbcServiceUri) {
        return services.inverse().get(bbcServiceUri);
    }

}
