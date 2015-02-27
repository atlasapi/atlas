package org.atlasapi.remotesite.opta.events.sports.model;

import java.lang.reflect.Type;
import java.util.List;

import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamData.TeamDataAttributes;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;


public class SportsTeamDataDeserializer implements JsonDeserializer<SportsTeamData> {

    @Override
    public SportsTeamData deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObj = json.getAsJsonObject();
        
        String value = context.deserialize(jsonObj.get("@value"), String.class);
        List<Goal> goals = ImmutableList.<Goal>of();
        TeamDataAttributes attributes = context.deserialize(jsonObj.get("@attributes"), TeamDataAttributes.class);
        
        return new SportsTeamData(value, goals, attributes);
    }

}
