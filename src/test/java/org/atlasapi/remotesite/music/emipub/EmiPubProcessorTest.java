package org.atlasapi.remotesite.music.emipub;

import java.io.File;
import java.util.List;

import org.atlasapi.media.entity.Song;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.google.common.base.Splitter;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.internal.matchers.CapturingMatcher;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 */
public class EmiPubProcessorTest {

    @Test
    public void testProcess() throws Exception {
        File data = new File(this.getClass().getClassLoader().getResource("emi_publishing.csv").getFile());

        AdapterLog log = mock(AdapterLog.class);
        ContentWriter contentWriter = mock(ContentWriter.class);
        
        CapturingMatcher<Song> capture = new CapturingMatcher<Song>();

        doReturn(null).when(contentWriter).createOrUpdate(argThat(capture));

        EmiPubProcessor processor = new EmiPubProcessor();
        processor.process(data, log, contentWriter);

        List<Song> actualSongs = capture.getAllValues();

        boolean firstSongFound = false;
        boolean secondSongFound = false;

        for (Song actualSong : actualSongs) {
            switch (actualSong.getCanonicalUri()){
            case "http://emimusicpub.com/works/325228":
                assertThat(actualSong.getPeople().size(), is(1));
                assertThat(
                        actualSong.getVersions().iterator().next().getRestriction().getMessage(),
                        restrictionMessageIs("Synchronisation Right:100.0,"
                                + "Performing Right:50.0,"
                                + "Material Change/Adaptation:100.0,"
                                + "Mechanical Right:100.0,"
                                + "Other:100.0"
                        )
                );
                firstSongFound = true;
                break;
            case "http://emimusicpub.com/works/1380618":
                assertThat(actualSong.getPeople().size(), is(2));
                assertThat(
                        actualSong.getVersions().iterator().next().getRestriction().getMessage(),
                        restrictionMessageIs("Synchronisation Right:100.0,"
                                + "Performing Right:50.0,"
                                + "Material Change/Adaptation:100.0,"
                                + "Mechanical Right:100.0,"
                                + "Other:100.0"
                        )
                );
                secondSongFound = true;
                break;
            default:
                fail();
            }
        }

        assertThat(firstSongFound, is(true));
        assertThat(secondSongFound, is(true));
    }

    private Matcher<? super String> restrictionMessageIs(final String expected) {
        return new TypeSafeMatcher<String>() {

            @Override
            public boolean matchesSafely(String actual) {
                List<String> parsedActual = parseMessage(actual);
                List<String> parsedExpected = parseMessage(expected);

                return parsedActual.size() == parsedExpected.size()
                        && parsedActual.containsAll(parsedExpected);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Expecting in any order: ")
                        .appendText("<")
                        .appendText(expected)
                        .appendText(">");
            }

            private List<String> parseMessage(String message) {
                return Splitter.on(',').splitToList(message);
            }
        };
    }
}
