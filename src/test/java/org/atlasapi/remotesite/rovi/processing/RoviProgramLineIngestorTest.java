package org.atlasapi.remotesite.rovi.processing;

import static org.atlasapi.remotesite.rovi.RoviCanonicalUriGenerator.canonicalUriForProgram;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import java.util.Set;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.rovi.RoviConstants;
import org.atlasapi.remotesite.rovi.RoviContentWriter;
import org.atlasapi.remotesite.rovi.RoviTestUtils;
import org.atlasapi.remotesite.rovi.parsers.RoviProgramLineParser;
import org.atlasapi.remotesite.rovi.populators.ContentPopulatorSupplier;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

@RunWith(MockitoJUnitRunner.class)
public class RoviProgramLineIngestorTest {

    @Mock
    private RoviContentWriter contentWriter;
    
    @Mock
    private ContentResolver contentResolver;
    
    @Mock 
    private ContentPopulatorSupplier contentPopulator;
    
    private RoviProgramLineIngestor ingestor;
    
    
    @Before
    public void init() {
        ingestor = new RoviProgramLineIngestor(
                new RoviProgramLineParser(),
                RoviConstants.FILE_CHARSET,
                Predicates.alwaysTrue(),
                contentWriter,
                contentResolver,
                contentPopulator);
    }
    
    @Test
    public void testContentKeepsBroadcastsWhenReingested() {
        // Existent broadcasts
        Broadcast broadcast = new Broadcast("", DateTime.now(), Duration.standardMinutes(60));
        broadcast.withId("br123");
        Set<Broadcast> broadcasts = ImmutableSet.of(broadcast);

        // Existent version
        Version version = new Version();
        version.setBroadcasts(broadcasts);

        // Existent film
        Film existentFilm = new Film();
        String canonicalUri = canonicalUriForProgram("15354310");
        existentFilm.setCanonicalUri(canonicalUri);
        existentFilm.setVersions(ImmutableSet.of(version));
        
        // Instruct resolver
        Mockito.when(contentResolver.findByCanonicalUris(ImmutableList.of(canonicalUri))).thenReturn(RoviTestUtils.resolvedContent(existentFilm));
        
        RoviProgramLineParser parser = new RoviProgramLineParser();
        String line = "MO|15354310||||15354310|3718664|Y|2|Puritan: European Flings|Puritan: European Flings|Puritan: European Flings|Puritan: Europe|Puritan:|||||Puritan: European Flings||Movie||||0|2007|en|N||None|None||Theatrical|Color|||Ins|||1875";
        
        Film created = (Film) ingestor.createContent(parser.apply(line));
        assertThat(Iterables.getOnlyElement(created.getVersions()).getBroadcasts(), hasItem(broadcast));
    }
    
}
