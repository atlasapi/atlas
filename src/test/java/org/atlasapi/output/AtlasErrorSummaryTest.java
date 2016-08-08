package org.atlasapi.output;

import java.io.IOException;
import java.io.StringReader;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolationException;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.input.DefaultJacksonModelReader;
import org.atlasapi.input.ModelReader;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.testing.ItemTestDataBuilder;

import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class AtlasErrorSummaryTest {

    private ModelReader reader = new DefaultJacksonModelReader();
    private JsonTranslator<Item> writer = new JsonTranslator<>();


    @Test
    public void constraintViolationsTest() throws IOException, ReadException {
        Item item = ItemTestDataBuilder.item().build();

        item.setUri(null);

        HttpServletRequest request = new StubHttpServletRequest();
        StubHttpServletResponse response = new StubHttpServletResponse();
        writer.writeTo(request, response, item, ImmutableSet.copyOf(Annotation.values()), ApplicationConfiguration
                .defaultConfiguration());

        String respBody = response.getResponseAsString();


        try {
            reader.read(new StringReader(respBody), Description.class, Boolean.TRUE);
            fail();
        } catch (ConstraintViolationException exception) {

            AtlasErrorSummary errorSummary = AtlasErrorSummary.forException(exception);
            assertEquals("INVALID_JSON_OBJECT", errorSummary.errorCode());
            assertEquals(1, errorSummary.violations().size());
        }

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
            reader.read(new StringReader(jsonString), Description.class, Boolean.TRUE);
            fail();
        } catch (JsonParseException exception) {
            AtlasErrorSummary summary = AtlasErrorSummary.forException(exception);
            assertEquals(Boolean.TRUE, summary.message().startsWith("Unexpected character"));
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
            reader.read(new StringReader(respBody), Description.class, Boolean.TRUE);
            fail();
        } catch (UnrecognizedPropertyException exception) {
            AtlasErrorSummary summary = AtlasErrorSummary.forException(exception);
            assertEquals(Boolean.TRUE, summary.message().startsWith("Unrecognized field \"hello\""));
        }

    }

}