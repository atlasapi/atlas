package org.atlasapi.remotesite.opta.events.sports.model;

import java.lang.reflect.Type;

import org.atlasapi.remotesite.opta.events.sports.model.SportsMatchInfo.MatchDate;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/**
 * This deserializer attempts to deal with the various ways in which dates are formatted in
 * Opta's event feed output. Depending upon the feed/record being parsed, the date string may
 * either be a string value or nested within another JSON object.
 * 
 * @author Oliver Hall (oli@metabroadcast.com)
 *
 */
public class MatchDateDeserializer implements JsonDeserializer<MatchDate> {

    @Override
    public MatchDate deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        if (json.isJsonObject()) {
            // date is on nested @value field
            JsonObject jsonObj = json.getAsJsonObject();
            return new MatchDate(jsonObj.get("@value").getAsString());
        } else {
            // date is string value
            return new MatchDate(json.getAsString());
        }
    }

}
