package org.atlasapi.remotesite.opta.events.sports;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.atlasapi.remotesite.opta.events.OptaDataTransformer;
import org.atlasapi.remotesite.opta.events.OptaEventsData;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeed;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeam;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public final class OptaSportsDataTransformer implements OptaDataTransformer<SportsTeam, SportsMatchData> {

    private final Gson gson = new GsonBuilder()
            .create();

    @Override
    public OptaEventsData<SportsTeam, SportsMatchData> transform(InputStream input) {
        OptaSportsEventsFeed eventsFeed = gson.fromJson(new InputStreamReader(input), OptaSportsEventsFeed.class);
        return new OptaSportsEventsData(eventsFeed.feed().document().matchData(), eventsFeed.feed().document().teams());
    }
    
    
}
