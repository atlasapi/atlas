package org.atlasapi.remotesite.opta.events.sports.model;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.lang.reflect.Type;

import org.atlasapi.remotesite.opta.events.sports.model.OptaSportsEventsFeed.OptaDocument;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;


public class OptaSportsEventsFeedDeserializer implements JsonDeserializer<OptaSportsEventsFeed> {

    private final String documentKey;
    
    public OptaSportsEventsFeedDeserializer(String documentKey) {
        this.documentKey = checkNotNull(documentKey);
    }

    @Override
    public OptaSportsEventsFeed deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        JsonElement innerDoc = json.getAsJsonObject().get(documentKey);
        return new OptaSportsEventsFeed(context.<OptaDocument>deserialize(innerDoc.getAsJsonObject(), OptaDocument.class));
    }

}
