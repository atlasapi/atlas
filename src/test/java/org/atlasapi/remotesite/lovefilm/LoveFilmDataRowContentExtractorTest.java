package org.atlasapi.remotesite.lovefilm;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URL;
import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.remotesite.lovefilm.LoveFilmData.LoveFilmDataRow;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

public class LoveFilmDataRowContentExtractorTest {

    private final LoveFilmDataRowContentExtractor extractor = new LoveFilmDataRowContentExtractor();
    
    @Test
    public void testExtractsSerialEpisodeWithEpisodeTitle() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("red-dwarf-s07-e02.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Episode episode = (Episode) content;
        
        assertThat(episode.getContainer().getUri(), endsWith("shows/179260"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("seasons/182909"));
        assertThat(episode.getEpisodeNumber(), is(2));
        assertThat(episode.getTitle(), is("Stoke Me a Clipper"));
        assertThat(episode.getDescription(), is("Rimmer's alter ego, Ace, arrives on Starbug badly wounded."));
        assertEquals(Specialization.TV, episode.getSpecialization());
        
        Set<String> expectedGenres = ImmutableSet.of(
            "http://lovefilm.com/genres/comedy",
            "http://lovefilm.com/genres/television",
            "http://lovefilm.com/genres/sci-fi-fantasy",
            "http://lovefilm.com/genres/sci-fi-fantasy/comedy", 
            "http://lovefilm.com/genres/comedy/sci-fi-fantasy",
            "http://lovefilm.com/genres/comedy/television",
            "http://lovefilm.com/genres/television/bbc",
            "http://lovefilm.com/genres/television/comedy"
            );
        
        assertEquals(expectedGenres, episode.getGenres());
    }
    
    @Test
    public void testExtractsSerialEpisodeWithoutEpisodeTitle() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("secret-diary-s02-e01.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Episode episode = (Episode) content;
        
        assertThat(episode.getContainer().getUri(), endsWith("shows/180765"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("seasons/180775"));
        assertThat(episode.getEpisodeNumber(), is(1));
        assertThat(episode.getTitle(), is("Episode 1"));
        assertEquals(Specialization.TV, episode.getSpecialization());
    }
    
    @Test
    public void testExtractsEpisodeTitleWithHyphenatedBrandTitle() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("mscl-s01-e16.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Episode episode = (Episode) content;
        
        assertThat(episode.getContainer().getUri(), endsWith("shows/188387"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("seasons/188388"));
        assertThat(episode.getEpisodeNumber(), is(16));
        assertThat(episode.getTitle(), is("Resolutions"));
        assertEquals(Specialization.TV, episode.getSpecialization());
    }
    
    @Test
    public void testExtractsSeriesTitleAndRemovesBrand() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("peep-show-s4.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Series series = (Series) content;
        
        assertThat(series.getTitle(), is("Series 4"));
    }
    
    /* TODO
     * This has been disabled as the example below now has a series in the latest lovefilm data.
     * It may well be that this case no longer exists.
     */
    @Ignore
    @Test
    public void testExtractsNonSerialEpisode() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("human-planet-e08.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Episode episode = (Episode) content;
        
        assertThat(episode.getContainer().getUri(), endsWith("192242"));
        assertThat(episode.getSeriesRef(), is(nullValue()));
        assertThat(episode.getEpisodeNumber(), is(8));
        assertThat(episode.getGenres(), hasItem("http://lovefilm.com/genres/specialinterest"));
        assertThat(episode.getTitle(), is("Cities Surviving the Urban Jungle"));
        assertEquals(Specialization.TV, episode.getSpecialization());
    }
    
    @Test
    public void testExtractsEpisodeWithoutSequenceTitle() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("blackadder-special.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Item episode = (Item) content;
        
        assertThat(episode.getContainer().getUri(), endsWith("177351"));
        assertThat(episode.getTitle(), is("Blackadder's Christmas Carol"));
        assertEquals(Specialization.TV, episode.getSpecialization());
    }

    /* TODO
     * Check whether this case still applies, and if so find an appropriate case to support it.
     */
    @Ignore
    @Test
    public void testExtractsTopLevelSeasonAsBrand() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("blackadder-specials.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Brand episode = (Brand) content;
        
        assertThat(episode.getTitle(), is("Blackadder - Special"));
        assertEquals(Specialization.TV, episode.getSpecialization());
    }

    @Test
    public void testExtractsMovieAsFilm() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("battle-royale.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Film film = (Film) content;
        
        assertThat(film.getTitle(), is("Battle Royale"));
        assertThat(film.getCanonicalUri(), endsWith("168818"));
        assertEquals(Specialization.FILM, film.getSpecialization());
        
        assertEquals(ImmutableSet.of(new Alias("gb:amazon:asin", "B00995Y076"), new Alias("zz:imdb:id", "tt0266308")), film.getAliases());
        assertEquals(ImmutableSet.of("http://gb.amazon.com/asin/B00995Y076", "http://www.imdb.com/title/tt0266308"), film.getAliasUrls());
    }

    @Test
    public void testNoAvailabilitiesSetsActivelyPublishedToFalse() throws IOException {
        
        LoveFilmDataRow row = rowFromFile("battle-royale-no-availability.csv");
        
        Optional<Content> extracted = extractor.extract(row);
        
        Content content = extracted.get();
        
        Film film = (Film) content;
        
        assertFalse(film.isActivelyPublished());
    }
    
    @Test
    public void testHyphenatedFilmTitleExtraction() throws Exception {
        
        LoveFilmDataRow row = rowFromFile("legends-in-concert.csv");
        
        assertThat(extractRow(row).getTitle(), is("Al Martino - Legends in Concert"));
        
    }

    @Test
    public void testHyphenatedEpisodeTitleExtraction() throws Exception {
        
        LoveFilmDataRow row = rowFromFile("knight-rider-goliath.csv");
        
        assertThat(extractRow(row).getTitle(), is("Goliath - Part 1"));
        
    }

    private Content extractRow(LoveFilmDataRow row) {
        return extractor.extract(row).get();
    }
    
    private LoveFilmDataRow rowFromFile(String filename) throws IOException {
        URL testFile = Resources.getResource(getClass(), filename);
        LoveFilmData data = new LoveFilmData(Resources.newReaderSupplier(testFile, Charsets.UTF_8));
        return data.processData(new LoveFilmDataProcessor<LoveFilmDataRow>() {

            private LoveFilmDataRow row;

            @Override
            public boolean process(LoveFilmDataRow row) {
                this.row = row;
                return false;
            }

            @Override
            public LoveFilmDataRow getResult() {
                return row;
            }
            
        });
    }

}
