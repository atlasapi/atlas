package org.atlasapi.remotesite.opta.events.soccer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.atlasapi.remotesite.opta.events.sports.OptaSportsEventsData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.io.Resources;


public class OptaSoccerDataTransformerTest {

    private final OptaSoccerDataTransformer transformer = new OptaSoccerDataTransformer();
    
    @Test
    public void testConversionToOptaSportsFormat() throws IOException {
        InputStream stream = streamFromFile("bundesliga_feed.json");
        OptaSportsEventsData data = (OptaSportsEventsData) transformer.transform(stream);
        
        assertEquals(2, Iterables.size(data.teams()));
        assertEquals(1, Iterables.size(data.matches()));
        
        SportsMatchData match = Iterables.getOnlyElement(data.matches());
        
        assertEquals("g758690", match.attributes().uId());
    }
    
    private InputStream streamFromFile(String filename) throws IOException {
        URL testFile = Resources.getResource(getClass(), filename);
        return testFile.openStream();
    }

}
