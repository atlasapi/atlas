package org.atlasapi.remotesite.music.emipub;

import java.io.File;
import org.atlasapi.media.entity.Song;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.mockito.internal.matchers.CapturingMatcher;

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
        
        assertEquals("http://emimusicpub.com/works/325228",
                capture.getAllValues().get(0).getCanonicalUri());
        assertEquals(1,
                capture.getAllValues().get(0).getPeople().size());
        assertEquals("Synchronisation Right:100.0,Performing Right:50.0,Material Change/Adaptation:100.0,Mechanical Right:100.0,Other:100.0",
                capture.getAllValues().get(0).getVersions().iterator().next().getRestriction().getMessage());
        
        assertEquals("http://emimusicpub.com/works/1380618",
                capture.getAllValues().get(1).getCanonicalUri());
        assertEquals(2,
                capture.getAllValues().get(1).getPeople().size());
        assertEquals("Synchronisation Right:100.0,Performing Right:50.0,Material Change/Adaptation:100.0,Mechanical Right:100.0,Other:100.0",
                capture.getAllValues().get(1).getVersions().iterator().next().getRestriction().getMessage());
    }
}
