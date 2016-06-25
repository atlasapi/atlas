package org.atlasapi.input;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Playlist;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class DefaultJacksonModelReader extends JacksonModelReader {


    private static ObjectMapper mapper;

    static {
        SimpleModule genericModule = new SimpleModule(
                "GenericDeserializerModule",
                new Version(0, 0, 1, null)
        );

        genericModule.addDeserializer(Date.class, new DateDeserializer());
        genericModule.addDeserializer(Description.class, new DescriptionDeserializer());
        genericModule.addDeserializer(Boolean.class, new BooleanDeserializer());
        genericModule.addDeserializer(ContentIdentifier.class, new ContentIdentifierDeserializer());
        genericModule.addDeserializer(Country.class, new CountryDeserializer());

        mapper = new ObjectMapper();

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.setPropertyNamingStrategy(
                PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        mapper.registerModule(genericModule);
        mapper.registerModule(new GuavaModule());
    }


    public DefaultJacksonModelReader() {
        super(mapper);
    }


    public static class DescriptionDeserializer extends JsonDeserializer<Description> {

        private Set<String> containerTypes = ImmutableSet.of("brand", "series");

        @Override
        public Description deserialize(JsonParser parser,
                DeserializationContext context)
                throws IOException {
            ObjectMapper mapper = (ObjectMapper) parser.getCodec();

            JsonNode node = mapper.readTree(parser);

            String type = node.get("type").textValue();

            if (Strings.isNullOrEmpty(type)) {
                throw new RuntimeException("Missing type");
            }

            if (containerTypes.contains(type)) {
                return mapper.treeToValue(node, Playlist.class);

            }
            return mapper.treeToValue(node, Item.class);

        }
    }

    public static class DateDeserializer extends JsonDeserializer<Date> {

        private static final DateTimeFormatter fmt = ISODateTimeFormat.dateTimeNoMillis();

        @Override
        public Date deserialize(JsonParser jsonParser,
                DeserializationContext context)
                throws IOException {

            String jsonString = jsonParser.getText();

            if (Strings.isNullOrEmpty(jsonString)) {
                return null;
            }

            return fmt.parseDateTime(jsonString).toDate();
        }
    }


    public static class BooleanDeserializer extends JsonDeserializer<Boolean> {

        private Map<String, Boolean> boolMap = ImmutableMap.of(
                "true", Boolean.TRUE,
                "false", Boolean.FALSE,
                "1", Boolean.TRUE,
                "0", Boolean.FALSE
        );

        @Override
        public Boolean deserialize(JsonParser jsonParser,
                DeserializationContext context)
                throws IOException {
            return boolMap.get(jsonParser.getText());
        }
    }

    public static class ContentIdentifierDeserializer extends JsonDeserializer<ContentIdentifier> {

        @Override
        public ContentIdentifier deserialize(JsonParser parser,
                DeserializationContext context)
                throws IOException {

            JsonNode node = parser.getCodec().readTree(parser);

            String uri = node.get("uri").asText();
            String type = node.get("type").asText();

            JsonNode idNode = node.get("id");
            String id = idNode != null ? idNode.asText() : null;

            if("series".equals(type)) {

                JsonNode seriesNode = node.get("series_number");
                Integer seriesNumber = seriesNode != null ? seriesNode.asInt() : null;
                return ContentIdentifier.seriesIdentifierFrom(id, uri, seriesNumber);


            }
            return ContentIdentifier.identifierFrom(id, uri, type);

        }
    }

    public static class CountryDeserializer extends JsonDeserializer<Country> {

        @Override
        public Country deserialize(JsonParser parser, DeserializationContext context)
                throws IOException {

            JsonNode node = parser.getCodec().readTree(parser);

            String jsonString = node.get("code").textValue();

            return Countries.fromCode(jsonString);

        }
    }

}
