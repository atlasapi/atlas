package org.atlasapi.remotesite.opta.events.sports;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.atlasapi.remotesite.opta.events.OptaDataTransformer;
import org.atlasapi.remotesite.opta.events.OptaEventsData;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeed;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeed.OptaDocument;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchInfo.MatchDate;
import org.atlasapi.remotesite.opta.events.sports.model.MatchDateDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.OptaDocumentDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeedDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchDataDeserializer;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeam;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamDataDeserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public final class OptaSportsDataTransformer implements OptaDataTransformer<SportsTeam, SportsMatchData> {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(OptaSportsEventsFeed.class, new OptaSportsEventsFeedDeserializer("OptaDocument"))
            .registerTypeAdapter(OptaDocument.class, new OptaDocumentDeserializer("OptaDocument"))
            .registerTypeAdapter(SportsMatchData.class, new SportsMatchDataDeserializer())
            .registerTypeAdapter(SportsTeamData.class, new SportsTeamDataDeserializer())
            .registerTypeAdapter(MatchDate.class, new MatchDateDeserializer())
            .create();

    @Override
    public OptaEventsData<SportsTeam, SportsMatchData> transform(InputStream input) {
        OptaSportsEventsFeed eventsFeed = gson.fromJson(new InputStreamReader(input), OptaSportsEventsFeed.class);
        return new OptaSportsEventsData(eventsFeed.feed().document().matchData(), eventsFeed.feed().document().teams());
    }
}
