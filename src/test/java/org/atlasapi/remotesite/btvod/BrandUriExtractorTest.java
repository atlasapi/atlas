package org.atlasapi.remotesite.btvod;


import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BrandUriExtractorTest {

    private static final String URI_PREFIX = "http://example.org/";
    private static final String BRAND_TITLE = "Brand Title";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = BRAND_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private final BrandUriExtractor brandUriExtractor = new BrandUriExtractor(URI_PREFIX, new TitleSanitiser());

    @Test
    public void testCanParseBrandFromSeasonTitles() {
        BtVodEntry row1 = seasonEntry();
        row1.setTitle("Dominion: S2");

        BtVodEntry row2 = seasonEntry();
        row2.setTitle("Workaholics: S2 - HD");

        BtVodEntry row3 = seasonEntry();
        row3.setTitle("Wire Series 5");

        BtVodEntry row4 = seasonEntry();
        row4.setTitle("Judge Geordie");

        BtVodEntry row5 = seasonEntry();
        row5.setTitle("Tom and Jerry Series 1");

        BtVodEntry row6 = seasonEntry();
        row6.setTitle("Plankton Invasion - Sr1 Series 1");

        BtVodEntry row7 = seasonEntry();
        row7.setTitle("Transformers Prime: Beast Hunters Series 3");

        assertThat(brandUriExtractor.extractBrandUri(row1).get(), is(URI_PREFIX + "synthesized/brands/dominion"));
        assertThat(brandUriExtractor.extractBrandUri(row2).get(), is(URI_PREFIX + "synthesized/brands/workaholics"));
        assertThat(brandUriExtractor.extractBrandUri(row3).get(), is(URI_PREFIX + "synthesized/brands/wire"));
        assertThat(brandUriExtractor.extractBrandUri(row4).get(), is(URI_PREFIX + "synthesized/brands/judge-geordie"));
        assertThat(brandUriExtractor.extractBrandUri(row5).get(), is(URI_PREFIX + "synthesized/brands/tom-and-jerry"));
        assertThat(brandUriExtractor.extractBrandUri(row6).get(), is(URI_PREFIX + "synthesized/brands/plankton-invasion"));
        assertThat(brandUriExtractor.extractBrandUri(row7).get(), is(URI_PREFIX + "synthesized/brands/transformers-prime-beast-hunters"));
    }

    //
    @Test
    public void testCanParseBrandFromEpisodeTitles() {
        BtVodEntry row1 = episodeEntry();
        row1.setTitle("Cashmere Mafia S1-E2 Conference Call");

        BtVodEntry row2 = episodeEntry();
        row2.setTitle(FULL_EPISODE_TITLE);

        BtVodEntry row3 = episodeEntry();
        row3.setTitle("Classic Premiership Rugby - Saracens v Leicester Tigers 2010/11");

        BtVodEntry row4 = episodeEntry();
        row4.setTitle("UFC: The Ultimate Fighter Season 19 - Season 19 Episode 2");

        BtVodEntry row5 = episodeEntry();
        row5.setTitle("Modern Family: S03 - HD S3-E17 Truth Be Told - HD");

        BtVodEntry row6 = episodeEntry();
        row6.setTitle("Being Human (USA) S2-E7 The Ties That Blind");

        BtVodEntry row7 = episodeEntry();
        row7.setTitle("ZQWModern_Family: S01 S1-E4 ZQWThe Incident");

        BtVodEntry row8 = episodeEntry();
        row8.setTitle("ZQZPeppa Pig: S01 S1-E4 ZQZSchool Play");

        BtVodEntry row9 = episodeEntry();
        row9.setTitle("ZQWAmerican_Horror_Story: S01 S1-E11 ZQWBirth");

        BtVodEntry row10 = episodeEntry();
        row10.setTitle("The Hunchback of Notre Dame II (Disney) - HD");

        BtVodEntry row11 = episodeEntry();
        row11.setTitle("Classic Premiership Rugby - Saracens v Leicester Tigers 2010/11 - HD");

        BtVodEntry row12 = episodeEntry();
        row12.setTitle("Plankton Invasion - Operation Yellow-Ball-That-Heats/ Operation Albedo/ Operation Meltdown");

        BtVodEntry row13 = episodeEntry();
        row13.setTitle("Brand - with multiple - dashes");

        BtVodEntry row14 = episodeEntry();
        row14.setTitle("Peppa Pig, Series 2, Vol. 1 - The Quarrel / The Toy Cupboard");

        BtVodEntry row15 = episodeEntry();
        row15.setTitle("Plankton Invasion - Sr1 S1-E46 Operation Some Like It Cold");

        assertThat(brandUriExtractor.extractBrandUri(row1).get(), is(URI_PREFIX + "synthesized/brands/cashmere-mafia"));
        assertThat(brandUriExtractor.extractBrandUri(row2).get(), is(URI_PREFIX + "synthesized/brands/brand-title"));
        assertThat(brandUriExtractor.extractBrandUri(row3).get(), is(URI_PREFIX + "synthesized/brands/classic-premiership-rugby"));
        assertThat(brandUriExtractor.extractBrandUri(row4).get(), is(URI_PREFIX + "synthesized/brands/ufc-the-ultimate-fighter"));
        assertThat(brandUriExtractor.extractBrandUri(row5).get(), is(URI_PREFIX + "synthesized/brands/modern-family"));
        assertThat(brandUriExtractor.extractBrandUri(row6).get(), is(URI_PREFIX + "synthesized/brands/being-human-usa"));
        assertThat(brandUriExtractor.extractBrandUri(row7).get(), is(URI_PREFIX + "synthesized/brands/modern-family"));
        assertThat(brandUriExtractor.extractBrandUri(row8).get(), is(URI_PREFIX + "synthesized/brands/peppa-pig"));
        assertThat(brandUriExtractor.extractBrandUri(row9).get(), is(URI_PREFIX + "synthesized/brands/american-horror-story"));
        assertThat(brandUriExtractor.extractBrandUri(row10).isPresent(), is(false));
        assertThat(brandUriExtractor.extractBrandUri(row11).get(), is(URI_PREFIX + "synthesized/brands/classic-premiership-rugby"));
        assertThat(brandUriExtractor.extractBrandUri(row12).get(), is(URI_PREFIX + "synthesized/brands/plankton-invasion"));
        assertThat(brandUriExtractor.extractBrandUri(row13).get(), is(URI_PREFIX + "synthesized/brands/brand"));
        assertThat(brandUriExtractor.extractBrandUri(row14).get(), is(URI_PREFIX + "synthesized/brands/peppa-pig"));
        assertThat(brandUriExtractor.extractBrandUri(row15).get(), is(URI_PREFIX + "synthesized/brands/plankton-invasion"));
    }

    private BtVodEntry episodeEntry() {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductType("episode");
        return entry;
    }

    private BtVodEntry seasonEntry() {
        BtVodEntry entry = new BtVodEntry();
        entry.setProductType("season");
        return entry;
    }

}