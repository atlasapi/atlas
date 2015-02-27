package org.atlasapi.remotesite.opta.events.soccer;

import java.lang.reflect.Type;
import java.util.List;

import org.atlasapi.remotesite.opta.events.sports.model.Goal;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamData;
import org.atlasapi.remotesite.opta.events.sports.model.SportsTeamData.TeamDataAttributes;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;


public class SoccerTeamDataDeserializer implements JsonDeserializer<SportsTeamData> {

    @Override
    public SportsTeamData deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        
        JsonObject jsonObj = json.getAsJsonObject();
        
        String value = context.deserialize(jsonObj.get("@value"), String.class);
        List<Goal> goals = parseGoals(jsonObj, context);
        TeamDataAttributes attributes = context.deserialize(jsonObj.get("@attributes"), TeamDataAttributes.class);
        
        return new SportsTeamData(value, goals, attributes);
    }
    
    /**
     * This is needed because Opta like to turn a singleton array of Goal objects into just an object,
     * which Gson understandably can't deserialize on its own. They also will not add the 'Goal' key at
     * all if there aren't any Goals records.
     * @param jsonObj
     * @param context
     * @return
     */
    private List<Goal> parseGoals(JsonObject jsonObj, final JsonDeserializationContext context) {
        if (!jsonObj.has("Goal")) {
            return ImmutableList.of();
        }
        JsonElement goals = jsonObj.get("Goal");
        if (goals.isJsonArray()) {
            return FluentIterable.from(goals.getAsJsonArray())
                    .transform(new Function<JsonElement, Goal>() {
                        @Override
                        public Goal apply(JsonElement input) {
                            return context.deserialize(input, Goal.class);
                        }
                    }).toList();
        } else {
            return ImmutableList.of((Goal)context.deserialize(goals, Goal.class));
        }
    }

}
