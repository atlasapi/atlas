package org.atlasapi.remotesite.rovi.processing;

import static junit.framework.Assert.assertTrue;
import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForProgram;
import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForSeason;
import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForSeasonHistory;
import static org.atlasapi.remotesite.rovi.RoviTestUtils.fileFromResource;
import static org.atlasapi.remotesite.rovi.RoviTestUtils.resolvedContent;
import static org.atlasapi.remotesite.rovi.RoviTestUtils.unresolvedContent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LocalizedDescription;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.rovi.RoviConstants;
import org.atlasapi.remotesite.rovi.RoviTestUtils;
import org.atlasapi.remotesite.rovi.indexing.KeyedFileIndexer;
import org.atlasapi.remotesite.rovi.indexing.MapBasedKeyedFileIndexer;
import org.atlasapi.remotesite.rovi.model.RoviCulture;
import org.atlasapi.remotesite.rovi.model.RoviProgramLine;
import org.atlasapi.remotesite.rovi.parsers.RoviProgramLineParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;


@RunWith(MockitoJUnitRunner.class)
public class RoviDeltaIngestProcessorTest extends AbstractRoviIngestProcessorTest {

    private static final String EPISODE_PARENT_BRAND_ID = "22636949";
    private static final String SEASON_PARENT_BRAND_ID = "3667685";
    private static final String PARENT_FILM_ID = "1240090";
    
    private static final String FILM_ID_TO_UPDATE = "1240043";
    private static final String EPISODE_ID_TO_UPDATE = "22679911";
    private static final String PROGRAM_TO_DELETE = "19232006";
    
    private static final String SEASON_ID_TO_UPDATE = "22487447";
    private static final String SEASON_HISTORY_ID_TO_UPDATE = "141406";
    private static final String SEASON_HISTORY_ID_TO_DELETE = "149987";
    private static final String SEASON_ID_TO_DELETE = "2222222";

    private static final String PROGRAM_ID_WITH_DESC_TO_DEL = "15707085";
    private static final String PROGRAM_ID_WITH_DESC_TO_UPD = "19205125";

    private static final String EPISODE_WITH_SEQ_TO_UPD = "10081943";
    private static final String EPISODE_WITH_SEQ_TO_DEL = "19937257";
    
    private static final String PROGRAM_FILE = "org/atlasapi/remotesite/rovi/deltas/program.txt";
    private static final String PROGRAM_DESCRIPTION = "org/atlasapi/remotesite/rovi/deltas/program_description.txt";
    private static final String EPISODE_SEQUENCE = "org/atlasapi/remotesite/rovi/deltas/episode_sequence.txt";
    private static final String SEASON_HISTORY_SEQUENCE = "org/atlasapi/remotesite/rovi/deltas/season_history.txt";
    private static final String SCHEDULE = "org/atlasapi/remotesite/rovi/deltas/schedule.txt";
    
    private static final Locale GERMAN_LOCALE = RoviCulture.localeFromCulture("German Generic");
    private static final Locale ENGLISH_NA_LOCALE = RoviCulture.localeFromCulture("English - NA");
    private static final Locale SPANISH_LOCALE = RoviCulture.localeFromCulture("Spanish Generic");
    
    private RoviDeltaIngestProcessor processor;

    @Before
    public void init() {
        instructContentResolver();

        processor = new RoviDeltaIngestProcessor(
                programIndexer(),
                descriptionsIndexer(),
                episodeSequenceIndexer(),
                roviContentWriter,
                contentResolver,
                scheduleProcessor,
                new AuxiliaryCacheSupplier(contentResolver));
    }

    @Test
    public void testDeltaIngest() throws IOException {
        processor.process(fileFromResource(PROGRAM_FILE),
                fileFromResource(SEASON_HISTORY_SEQUENCE),
                fileFromResource(SCHEDULE),
                fileFromResource(PROGRAM_DESCRIPTION),
                fileFromResource(EPISODE_SEQUENCE));

        assertTrue(contentWriter.hasWritten());

        Map<String, ? extends Content> items = contentWriter.getItems();

        Content filmToInsert = items.get(canonicalUriForProgram("20049987"));
        assertThat(filmToInsert, notNullValue());
        assertThat(filmToInsert, is(Film.class));
        assertThat(filmToInsert.getTitle(), equalTo("Ikarus"));
        assertThat(filmToInsert.getPublisher(), equalTo(Publisher.ROVI_DE));
        assertThat(filmToInsert.getEquivalentTo().isEmpty(), is(true));
        assertThat(filmToInsert.getLocalizedDescription(GERMAN_LOCALE).get().getDescription(),
                equalTo("Lena ist heimatlos. In der Großstadt konnte sie nicht Fuß fassen, am Land ist sie längst nicht mehr zu Hause und die große, weite Welt, die sie angeblich als Kellnerin auf einem Kreuzfahrtschiff bereist hat, kennt sie nur vom Hörensagen. Das Begräbnis des Großvaters ist ihr nun Anlass und Vorwand zugleich, ins Dorf zurückzukehren, in der Hoffnung vielleicht doch hier ihren Platz im Leben zu finden."));

        Content filmToUpdate = items.get(canonicalUriForProgram(FILM_ID_TO_UPDATE));
        assertThat(filmToUpdate, notNullValue());
        assertThat(filmToUpdate, is(Film.class));
        assertThat(filmToUpdate.getTitle(), equalTo("Walking Thunder"));
        assertThat(filmToUpdate.getPublisher(), equalTo(Publisher.ROVI_EN));
        assertThat(filmToUpdate.getEquivalentTo(), hasItem(LookupRef.from(parentFilm())));

        Content brandToIns = items.get(canonicalUriForProgram(EPISODE_PARENT_BRAND_ID));
        assertThat(brandToIns, notNullValue());
        assertThat(brandToIns, is(Brand.class));
        assertThat(brandToIns.getTitle(), equalTo("Il trenino Thomas e i suoi amici"));
        assertThat(brandToIns.getPublisher(), equalTo(Publisher.ROVI_IT));
        assertThat(brandToIns.getEquivalentTo().isEmpty(), is(true));

        Content episodeToUpd = items.get(canonicalUriForProgram(EPISODE_ID_TO_UPDATE));
        assertThat(episodeToUpd, notNullValue());
        assertThat(episodeToUpd, is(Episode.class));
        assertThat(episodeToUpd.getTitle(), equalTo("Una doccia ci vuole"));
        assertThat(episodeToUpd.getPublisher(), equalTo(Publisher.ROVI_IT));
        assertThat(episodeToUpd.getEquivalentTo().isEmpty(), is(true));
        Episode episode = (Episode) episodeToUpd;
        assertThat(episode.getContainer(), equalTo(ParentRef.parentRefFrom(parentBrand(EPISODE_PARENT_BRAND_ID))));

        Content programToDelete = items.get(canonicalUriForProgram(PROGRAM_TO_DELETE));
        assertThat(programToDelete, notNullValue());
        assertThat(programToDelete, is(Film.class));
        assertThat(programToDelete.isActivelyPublished(), is(false));
        
        Content seasonToIns = items.get(canonicalUriForSeason("23435451"));
        assertThat(seasonToIns, notNullValue());
        assertThat(seasonToIns, is(Series.class));
        assertThat(seasonToIns.getTitle(), equalTo("Season 18"));
        assertThat(seasonToIns.getPublisher(), equalTo(Publisher.ROVI_EN));
        Series series = (Series) seasonToIns;
        assertThat(series.getParent(), equalTo(ParentRef.parentRefFrom(parentBrand(SEASON_PARENT_BRAND_ID))));
        assertThat(series.getSeriesNumber(), equalTo(18));

        Content seasonToUpd = items.get(canonicalUriForSeason(SEASON_ID_TO_UPDATE));
        assertThat(seasonToUpd, notNullValue());
        assertThat(seasonToUpd, is(Series.class));
        assertThat(seasonToUpd.getTitle(), equalTo("Season 17"));
        assertThat(seasonToUpd.getPublisher(), equalTo(Publisher.ROVI_EN));
        Series series2 = (Series) seasonToUpd;
        assertThat(series2.getParent(), equalTo(ParentRef.parentRefFrom(parentBrand(SEASON_PARENT_BRAND_ID))));
        assertThat(series2.getSeriesNumber(), equalTo(17));

        Content seasonToDel = items.get(canonicalUriForSeason(SEASON_ID_TO_DELETE));
        assertThat(seasonToDel, notNullValue());
        assertThat(seasonToDel, is(Series.class));
        assertThat(seasonToDel.isActivelyPublished(), is(false));
        
        Content filmWithDescToUpd = items.get(canonicalUriForProgram(PROGRAM_ID_WITH_DESC_TO_UPD));
        assertThat(filmWithDescToUpd, notNullValue());
        assertThat(filmWithDescToUpd, is(Film.class));
        assertThat(filmWithDescToUpd.getLocalizedDescription(ENGLISH_NA_LOCALE).get().getShortDescription(), equalTo("Discussing the Premier League."));
        assertThat(filmWithDescToUpd.getLocalizedDescription(ENGLISH_NA_LOCALE).get().getDescription(), equalTo("Discussing the Premier League."));

        Content filmWithDescToDel = items.get(canonicalUriForProgram(PROGRAM_ID_WITH_DESC_TO_DEL));
        assertThat(filmWithDescToDel, notNullValue());
        assertThat(filmWithDescToDel, is(Film.class));
        assertThat(filmWithDescToDel.getLocalizedDescriptions().isEmpty(), is(true));
        
        Content episodeWithSeqToUpd = items.get(canonicalUriForProgram(EPISODE_WITH_SEQ_TO_UPD));
        assertThat(episodeWithSeqToUpd, notNullValue());
        assertThat(episodeWithSeqToUpd, is(Episode.class));
        assertThat(episodeWithSeqToUpd.getTitle(), equalTo("Dark Horse"));

        Content episodeWithSeqToDel = items.get(canonicalUriForProgram(EPISODE_WITH_SEQ_TO_DEL));
        assertThat(episodeWithSeqToDel, notNullValue());
        assertThat(episodeWithSeqToDel, is(Episode.class));
        assertThat(episodeWithSeqToDel.getTitle(), equalTo("This title has to remain the same"));
    }

    private KeyedFileIndexer<String, RoviProgramLine> programIndexer() {
        return new MapBasedKeyedFileIndexer<>(
                RoviConstants.FILE_CHARSET,
                new RoviProgramLineParser());
    }
    
    private void instructContentResolver() {
        when(contentResolver.findByCanonicalUris(Mockito.anyCollectionOf(String.class)))
            .thenReturn(unresolvedContent());
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(EPISODE_PARENT_BRAND_ID))))
            .thenAnswer(writtenOrUnresolved(EPISODE_PARENT_BRAND_ID));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(SEASON_PARENT_BRAND_ID))))
            .thenReturn(resolvedContent(parentBrand(SEASON_PARENT_BRAND_ID)));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(PARENT_FILM_ID))))
            .thenReturn(resolvedContent(parentFilm()));

        // Resolving as an Item in order to test the content type change
        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(FILM_ID_TO_UPDATE))))
            .thenReturn(resolvedContent(new Item(canonicalUriForProgram(FILM_ID_TO_UPDATE), "", Publisher.ROVI_EN)));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(EPISODE_ID_TO_UPDATE))))
            .thenReturn(resolvedContent(new Episode(canonicalUriForProgram(EPISODE_ID_TO_UPDATE), "", Publisher.ROVI_IT)));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(PROGRAM_TO_DELETE))))
            .thenReturn(resolvedContent(new Film(canonicalUriForProgram(PROGRAM_TO_DELETE), "", Publisher.ROVI_EN)));

        when(contentResolver.findByUris(ImmutableList.of(canonicalUriForSeasonHistory(SEASON_HISTORY_ID_TO_UPDATE))))
            .thenReturn(resolvedContent(new Series(canonicalUriForSeason(SEASON_ID_TO_UPDATE), "", Publisher.ROVI_EN)));

        when(contentResolver.findByUris(ImmutableList.of(canonicalUriForSeasonHistory(SEASON_HISTORY_ID_TO_DELETE))))
            .thenReturn(resolvedContent(new Series(canonicalUriForSeason(SEASON_ID_TO_DELETE), "", Publisher.ROVI_EN)));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(PROGRAM_ID_WITH_DESC_TO_DEL))))
            .thenReturn(resolvedContent(filmWithDescToDelete()));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(PROGRAM_ID_WITH_DESC_TO_UPD))))
            .thenReturn(resolvedContent(filmWithDescToUpdate()));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(EPISODE_WITH_SEQ_TO_UPD))))
            .thenReturn(resolvedContent(episodeWithSeqToUpdate()));

        when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUriForProgram(EPISODE_WITH_SEQ_TO_DEL))))
            .thenReturn(resolvedContent(episodeWithSeqToDelete()));

        when(contentResolver.findByUris(Mockito.anyCollectionOf(String.class)))
                .thenReturn(RoviTestUtils.unresolvedContent());

        when(contentResolver.findByUris(ImmutableList.of(canonicalUriForSeasonHistory(SEASON_HISTORY_ID_TO_UPDATE))))
                .thenReturn(resolvedContent(new Series(canonicalUriForSeason(SEASON_ID_TO_UPDATE), "", Publisher.ROVI_EN_GB)));

        when(contentResolver.findByUris(ImmutableList.of(canonicalUriForSeasonHistory(SEASON_HISTORY_ID_TO_DELETE))))
                .thenReturn(resolvedContent(new Series(canonicalUriForSeason(SEASON_ID_TO_DELETE), "", Publisher.ROVI_EN_GB)));
    }

    private Episode episodeWithSeqToDelete() {
        Episode episodeWithSeqToDel = new Episode(canonicalUriForProgram(EPISODE_WITH_SEQ_TO_DEL), "", Publisher.ROVI_EN);
        episodeWithSeqToDel.setTitle("This title has to remain the same");
        return episodeWithSeqToDel;
    }


    private Episode episodeWithSeqToUpdate() {
        Episode episodeWithSeqToUpd = new Episode(canonicalUriForProgram(EPISODE_WITH_SEQ_TO_UPD), "", Publisher.ROVI_EN);
        episodeWithSeqToUpd.setTitle("This title has to be updated");
        return episodeWithSeqToUpd;
    }


    private Film filmWithDescToUpdate() {
        Film filmWithDescToUpdate = new Film(canonicalUriForProgram(PROGRAM_ID_WITH_DESC_TO_UPD), "", Publisher.ROVI_EN);
        
        LocalizedDescription description = new LocalizedDescription();
        description.setLocale(ENGLISH_NA_LOCALE);
        description.setLongDescription("This description must be updated");
        description.setDescription("This description must be updated");

        return filmWithDescToUpdate;
    }


    private Film filmWithDescToDelete() {
        Film filmWithDescToDelete = new Film(canonicalUriForProgram(PROGRAM_ID_WITH_DESC_TO_DEL), "", Publisher.ROVI_ES);
        
        LocalizedDescription description = new LocalizedDescription();
        description.setLocale(SPANISH_LOCALE);
        description.setLongDescription("This description must be deleted");
        description.setDescription("This description must be deleted");
        
        filmWithDescToDelete.addLocalizedDescription(description);
        
        return filmWithDescToDelete;
    }

    private Brand parentBrand(String id) {
        return basicBrand(id, Publisher.ROVI_EN);
    }

    private Film parentFilm() {
        return basicFilm(PARENT_FILM_ID, Publisher.ROVI_FR);
    }

    private Brand basicBrand(String id, Publisher publisher) {
        Brand brand = new Brand();
        brand.setCanonicalUri(canonicalUriForProgram(id));
        brand.setPublisher(publisher);
        return brand;
    }

    private Film basicFilm(String id, Publisher publisher) {
        Film film = new Film();
        film.setCanonicalUri(canonicalUriForProgram(id));
        film.setPublisher(publisher);
        return film;
    }
    
}
