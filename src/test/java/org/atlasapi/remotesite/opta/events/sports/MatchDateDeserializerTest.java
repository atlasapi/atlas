package org.atlasapi.remotesite.opta.events.sports;

import static org.junit.Assert.*;

import org.atlasapi.remotesite.opta.events.sports.model.MatchDateDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchInfo.MatchDate;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class MatchDateDeserializerTest {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(MatchDate.class, new MatchDateDeserializer())
            .create();
    
    @Test
    public void testDeserializesStringValueDate() {
        String dateString = "\"2014-09-21 14:30:00\"";
        MatchDate date = gson.fromJson(dateString, MatchDate.class);
        
        assertEquals("2014-09-21 14:30:00", date.date());
    }

    @Test
    public void testDeserializesDateFromDateObject() {
        String dateObjectString = "{ \"@attributes\": { \"TBC\": \"1\" }, \"@value\": \"2014-09-24 19:00:00\" }";
        MatchDate date = gson.fromJson(dateObjectString, MatchDate.class);
     
        assertEquals("2014-09-24 19:00:00", date.date());
    }
}
