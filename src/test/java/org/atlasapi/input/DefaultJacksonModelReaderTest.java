package org.atlasapi.input;

import java.io.IOException;
import java.io.StringReader;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.Broadcast;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Location;
import org.atlasapi.media.entity.simple.Person;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.media.entity.simple.TopicRef;
import org.atlasapi.media.entity.testing.ItemTestDataBuilder;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.JsonTranslator;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;
import com.metabroadcast.common.time.DateTimeZones;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DefaultJacksonModelReaderTest {

    private DefaultJacksonModelReader reader;
    private JsonTranslator<Item> writer;

    @Before
    public void setUp() {
        this.reader = new DefaultJacksonModelReader();
        this.writer = new JsonTranslator<>();
    }

    @Test
    public void testItemTranslator() throws IOException, ReadException {

        Item testItem = ItemTestDataBuilder.item()
                .build();
        TopicRef topicRef = new TopicRef();
        topicRef.setRelationship("about");
        testItem.setTopics(ImmutableSet.of(topicRef));
        testItem.setDuration(30L);

        Broadcast broadcast = new Broadcast("1", new DateTime(), new DateTime());

        DateTime now = new DateTime(DateTimeZones.UTC);
        Location location = new Location();
        location.setAvailabilityStart(now.minusHours(2).toDate());
        location.setActualAvailabilityStart(now.minusHours(1).toDate());
        location.setAvailabilityEnd(now.plusHours(2).toDate());

        testItem.addLocation(location);

        testItem.addBroadcast(broadcast);

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(
                request,
                response,
                testItem,
                ImmutableSet.copyOf(Annotation.values()),
                ApplicationConfiguration
                        .defaultConfiguration()
        );

        String respBody = response.getResponseAsString();

        Item actual = (Item) reader.read(new StringReader(respBody), Description.class);

        assertEquals(testItem.getUri(), actual.getUri());
        assertEquals(testItem.getDuration(), actual.getDuration());

    }

    @Test
    public void testNullUriItem() throws IOException, ReadException {
        Item testItem = ItemTestDataBuilder.item()
                .build();
        testItem.setUri(null);

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(
                request,
                response,
                testItem,
                ImmutableSet.copyOf(Annotation.values()),
                ApplicationConfiguration
                        .defaultConfiguration()
        );

        String respBody = response.getResponseAsString();
        try {
            reader.read(new StringReader(respBody), Description.class);
            fail();

        } catch (ConstraintViolationException exception) {
            ConstraintViolation<Item> violation =
                    (ConstraintViolation<Item>) Iterables.getOnlyElement(exception.getConstraintViolations());
            assertEquals("uri", violation.getPropertyPath().toString());
            assertEquals("may not be null", violation.getMessage());

        }

    }

    @Test
    public void testTranslateItemWithCountryCode() throws IOException, ReadException {
        Item testItem = ItemTestDataBuilder.item()
                .build();

        Country country = Countries.fromCode("US");

        testItem.setCountriesOfOrigin(ImmutableSet.of(country));
        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(
                request,
                response,
                testItem,
                ImmutableSet.copyOf(Annotation.values()),
                ApplicationConfiguration
                        .defaultConfiguration()
        );

        String respBody = response.getResponseAsString();

        Item description = (Item) reader.read(new StringReader(respBody), Description.class);

        Country actual = Iterables.getOnlyElement(description.getCountriesOfOrigin());

        assertEquals("US", actual.code());

    }

    @Test
    public void contentIdentifierTest() throws IOException, ReadException {

        Item testItem = ItemTestDataBuilder.item().build();
        ContentIdentifier.SeriesIdentifier seriesIdentifier =
                new ContentIdentifier.SeriesIdentifier("abc", testItem.getSeriesNumber(), "abc");

        testItem.setSimilarContent(ImmutableSet.of(seriesIdentifier));

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(
                request,
                response,
                testItem,
                ImmutableSet.copyOf(Annotation.values()),
                ApplicationConfiguration
                        .defaultConfiguration()
        );

        String respBody = response.getResponseAsString();

        Item description = (Item) reader.read(new StringReader(respBody), Description.class);

        ContentIdentifier actual = Iterables.getOnlyElement(description.getSimilarContent());

        assertEquals("series", actual.getType());

    }

    @Test
    public void parseSeriesTest() throws IOException, ReadException {

        Playlist item = new Playlist();
        item.setUri("abc");
        item.setType("series");

        JsonTranslator<Playlist> writer = new JsonTranslator<>();

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(
                request,
                response,
                item,
                ImmutableSet.copyOf(Annotation.values()),
                ApplicationConfiguration
                        .defaultConfiguration()
        );

        String respBody = response.getResponseAsString();

        Description description = reader.read(new StringReader(respBody), Description.class);

        assertEquals(Boolean.TRUE, description instanceof Playlist);

    }

    @Test
    public void parsePersonTest() throws IOException, ReadException {

        Person person = new Person();
        person.setType("person");
        person.setUri("abc");

        JsonTranslator<Person> writer = new JsonTranslator<>();

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(
                request,
                response,
                person,
                ImmutableSet.copyOf(Annotation.values()),
                ApplicationConfiguration
                        .defaultConfiguration()
        );

        String respBody = response.getResponseAsString();

        Person actual = reader.read(new StringReader(respBody), Person.class);

        assertEquals(person.getUri(), actual.getUri());

    }

    @Test
    public void jacksonParseExceptionTest() throws IOException, ReadException {
        String jsonString = "{"
                + "\"uri\" : \"abs\","
                + "\"description\" : \"description\","
                + "\"type\" : \"item\","
                + "\"locations\" : \"[]\","
                + "}";

        try {
            reader.read(new StringReader(jsonString), Description.class);
            fail();
        } catch (JsonParseException exception) {
            assertEquals(Boolean.TRUE, exception.getMessage().startsWith("Unexpected character"));
        }
    }

    @Test
    public void jacksonUnknownPropertiesExceptionTest() throws IOException, ReadException {
        Item item = ItemTestDataBuilder.item().build();

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(request, response, item, ImmutableSet.copyOf(Annotation.values()), ApplicationConfiguration
                .defaultConfiguration());

        String respBody = response.getResponseAsString().replace("locations", "hello");

        try {
            reader.read(new StringReader(respBody), Description.class);
            fail();
        } catch (UnrecognizedPropertyException exception) {
            assertEquals(Boolean.TRUE, exception.getMessage().startsWith("Unrecognized field \"hello\""));
        }

    }

}