package org.atlasapi.input;

import java.lang.reflect.Type;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Playlist;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;

public final class DefaultGsonModelReader extends GsonModelReader {

    private DefaultGsonModelReader() {
        super(new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .registerTypeAdapter(Date.class, new DateDeserializer())
            .registerTypeAdapter(Long.class, new LongDeserializer())
            .registerTypeAdapter(Boolean.class, new BooleanDeserializer())
            .registerTypeAdapter(ContentIdentifier.class, new ContentIdentifierDeserializer())
            .registerTypeAdapter(Country.class, new CountryDeserializer())
            .registerTypeAdapter(Description.class, new DescriptionDeserializer())
            .registerTypeAdapter(DateTime.class, new JodaDateTimeSerializer()));
    }

    public static DefaultGsonModelReader create() {
        return new DefaultGsonModelReader();
    }
    
    private static final class DescriptionDeserializer implements JsonDeserializer<Description> {
        
        private Set<String> containerTypes = ImmutableSet.of("brand","series");

        @Override
        public Description deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            String type = getTypeString(json);
            if (containerTypes.contains(type)) {
                return context.deserialize(json, Playlist.class);
            }
            return context.deserialize(json, Item.class);
        }

        protected String getTypeString(JsonElement json) {
            try {
                JsonPrimitive typeObj = json.getAsJsonObject().getAsJsonPrimitive("type");
                if (typeObj == null) {
                    throw new JsonParseException("missing type"); 
                }
                return typeObj.getAsString();
            } catch (IllegalStateException ise) {
                throw new JsonParseException(ise);
            }
        }
    }

    public static class DateDeserializer implements JsonDeserializer<Date> {

        private static final DateTimeFormatter fmt = ISODateTimeFormat.dateTimeNoMillis();

        @Override
        public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            String jsonString = json.getAsJsonPrimitive().getAsString();
            if (Strings.isNullOrEmpty(jsonString)) {
                return null;
            }
            return fmt.parseDateTime(jsonString).toDate();
        }
    }
    
    public static class LongDeserializer implements JsonDeserializer<Long> {

        @Override
        public Long deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            String jsonString = json.getAsJsonPrimitive().getAsString();
            if (Strings.isNullOrEmpty(jsonString)) {
                return null;
            }
            return json.getAsLong();
        }
    }
    
    public static class BooleanDeserializer implements JsonDeserializer<Boolean> {

        private Map<String, Boolean> boolMap = ImmutableMap.of(
                "true", Boolean.TRUE,
                "false", Boolean.FALSE,
                "1", Boolean.TRUE,
                "0", Boolean.FALSE);
        
        @Override
        public Boolean deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return boolMap.get(json.getAsJsonPrimitive().getAsString());
        }
    }

    public static class ContentIdentifierDeserializer implements JsonDeserializer<ContentIdentifier> {
        
        @Override
        public ContentIdentifier deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObj = json.getAsJsonObject();
            String uri = jsonObj.get("uri").getAsString();
            String type = jsonObj.get("type").getAsString();
            JsonElement idElement = jsonObj.get("id");
            String id = idElement != null ? idElement.getAsString() : null;
            
            if ("series".equals(type)) {
                JsonElement seriesElement = jsonObj.get("seriesNumber");
                Integer seriesNumber = seriesElement != null ? seriesElement.getAsInt() : null;
                return ContentIdentifier.seriesIdentifierFrom(id, uri, seriesNumber);
            } else {
                return ContentIdentifier.identifierFrom(id, uri, type);
            }
        }
    }
    
    public static class CountryDeserializer implements JsonDeserializer<Country> {
        
        @Override
        public Country deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return Countries.fromCode(json.getAsJsonObject().get("code").getAsString());
        }
    }
}
