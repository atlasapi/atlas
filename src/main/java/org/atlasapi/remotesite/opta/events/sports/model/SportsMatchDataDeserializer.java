package org.atlasapi.remotesite.opta.events.sports.model;

import java.lang.reflect.Type;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;


public class SportsMatchDataDeserializer implements JsonDeserializer<SportsMatchData> {

    @Override
    public SportsMatchData deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObj = json.getAsJsonObject();
        
        SportsMatchInfo matchInfo = context.deserialize(jsonObj.get("MatchInfo"), SportsMatchInfo.class);
        List<SportsStats> stats = deserializeStats(jsonObj.get("Stat"), context);
        List<SportsTeamData> teamData = deserializeTeams(jsonObj.get("TeamData"), context);
        MatchDataAttributes attributes = context.deserialize(jsonObj.get("@attributes"), MatchDataAttributes.class);
        
        return new SportsMatchData(matchInfo, stats, teamData, attributes);
    }
    
    private List<SportsStats> deserializeStats(JsonElement statsElem, final JsonDeserializationContext context) {
        if (statsElem.isJsonObject()) {
            // non-array stats
            JsonObject jsonObj = statsElem.getAsJsonObject();
            return ImmutableList.of(context.<SportsStats>deserialize(jsonObj, SportsStats.class));
        } else {
            // array stats
            JsonArray jsonArr = statsElem.getAsJsonArray();
            return FluentIterable.from(jsonArr)
                    .transform(new Function<JsonElement, SportsStats>() {
                        @Override
                        public SportsStats apply(JsonElement input) {
                            return context.<SportsStats>deserialize(input.getAsJsonObject(), SportsStats.class);
                        }
                    })
                    .toList();
        }
    }

    private List<SportsTeamData> deserializeTeams(JsonElement teamElem,
            final JsonDeserializationContext context) {
        return FluentIterable.from(teamElem.getAsJsonArray())
                .transform(new Function<JsonElement, SportsTeamData>() {
                    @Override
                    public SportsTeamData apply(JsonElement input) {
                        return context.<SportsTeamData>deserialize(input.getAsJsonObject(), SportsTeamData.class);
                    }
                })
                .toList();
    }


}
