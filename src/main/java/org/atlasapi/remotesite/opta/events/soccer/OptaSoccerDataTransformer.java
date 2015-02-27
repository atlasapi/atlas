package org.atlasapi.remotesite.opta.events.soccer;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.atlasapi.remotesite.opta.events.OptaDataTransformer;
import org.atlasapi.remotesite.opta.events.OptaEventsData;
import org.atlasapi.remotesite.opta.events.sports.OptaSportsEventsData;
import org.atlasapi.remotesite.opta.events.sports.model.MatchDateDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.OptaDocumentDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeed;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeed.OptaDocument;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeedDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchDataDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchInfo.MatchDate;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeam;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class OptaSoccerDataTransformer implements OptaDataTransformer<SportsTeam, SportsMatchData> {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(OptaSportsEventsFeed.class, new OptaSportsEventsFeedDeserializer("SoccerFeed"))
            .registerTypeAdapter(OptaDocument.class, new OptaDocumentDeserializer("SoccerDocument"))
            .registerTypeAdapter(SportsMatchData.class, new SportsMatchDataDeserializer())
            .registerTypeAdapter(SportsTeamData.class, new SoccerTeamDataDeserializer())
            .registerTypeAdapter(MatchDate.class, new MatchDateDeserializer())
            .create();

    @Override
    public OptaEventsData<SportsTeam, SportsMatchData> transform(InputStream input) {
        OptaSportsEventsFeed eventsFeed = gson.fromJson(new InputStreamReader(input), OptaSportsEventsFeed.class);
        return new OptaSportsEventsData(eventsFeed.feed().document().matchData(), eventsFeed.feed().document().teams());
    }
}
