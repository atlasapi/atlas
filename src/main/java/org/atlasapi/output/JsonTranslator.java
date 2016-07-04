/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.util.Date;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.Description;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Person;
import org.atlasapi.media.entity.simple.Playlist;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Charsets;
import com.google.common.io.Flushables;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.metabroadcast.common.http.HttpHeaders;
import com.metabroadcast.common.time.DateTimeZones;

/**
 * Outputs simple URIplay model in Json.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class JsonTranslator<T> implements AtlasModelWriter<T> {
    
    private static final String GZIP_HEADER_VALUE = "gzip";

	public static final String CALLBACK = "callback";

	private final Gson gson;

	public JsonTranslator() {
		this(new GsonBuilder()
					.disableHtmlEscaping()
					.setDateFormat(DateFormat.LONG)
					.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
					.registerTypeAdapter(AtlasErrorSummary.class, new AtlasExceptionJsonSerializer())
					.registerTypeAdapter(Date.class, new DateTimeSerializer())
					.registerTypeAdapter(Description.class, new DescriptionSerializer())
					.registerTypeAdapter(DateTime.class, new JodaDateTimeSerializer()));
	}

    public JsonTranslator(GsonBuilder gsonBuilder) {
        this.gson = gsonBuilder.create();
    }

    @Override
	public void writeTo(HttpServletRequest request, HttpServletResponse response, T model, Set<Annotation> annotations, ApplicationConfiguration config) throws IOException {

	    OutputStream out = response.getOutputStream();

	    String callback = callback(request);
		
		String accepts = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
        if (accepts != null && accepts.contains(GZIP_HEADER_VALUE)) {
            response.setHeader(HttpHeaders.CONTENT_ENCODING, GZIP_HEADER_VALUE);
            out = new GZIPOutputStream(out);
        }

        OutputStreamWriter writer = new OutputStreamWriter(out, Charsets.UTF_8);

		try {
			if (callback != null) {
				writer.write(callback + "(");
			}
			gson.toJson(model, writer);
			if (callback != null) {
				writer.write(");");
			}
		} finally {
			Flushables.flushQuietly(writer);
			if (out instanceof GZIPOutputStream) {
			    ((GZIPOutputStream) out).finish();
			}
		}
	}

	private String callback(HttpServletRequest request) {
		if (request == null) {
			return null;
		}
		String callback = request.getParameter(CALLBACK);
		if (callback == null || callback.length() < 1) {
			return null;
		}

		try {
			return URLEncoder.encode(callback, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	@Override
	public void writeError(HttpServletRequest request, HttpServletResponse response, AtlasErrorSummary exception) throws IOException {
		String callback = callback(request);

		OutputStreamWriter writer = new OutputStreamWriter(response.getOutputStream(), Charsets.UTF_8);

		try {
			if (callback != null) {
				writer.write(callback + "(");
			}
			writer.write(gson.toJson(exception));

			if (callback != null) {
				writer.write(");");
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				writer.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static class AtlasExceptionJsonSerializer implements JsonSerializer<AtlasErrorSummary> {

		@Override
		public JsonElement serialize(AtlasErrorSummary src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject serialized = new JsonObject();
			JsonObject error = new JsonObject();
			error.addProperty("message", src.message());
			error.addProperty("error_code", src.errorCode());
			error.addProperty("error_id", src.id());
			serialized.add("error", error);
			return serialized;// ;
		}

	}
	
	private static final class DescriptionSerializer implements JsonSerializer<Description> {

		@Override
		public JsonElement serialize(Description description, Type type, JsonSerializationContext context) {
			JsonElement element = null;
			if (description instanceof Item) {
				element = context.serialize(description, Item.class);
			} else if (description instanceof Person ) {
			    element = context.serialize(description, Person.class);
			} else if (description instanceof Playlist ) {
				element = context.serialize(description, Playlist.class);
			} else {
			    throw new IllegalArgumentException("Cannot serialise subclass of Description of type " + description.getClass().getName());
			}
			return element;
		}
	}

    private static final class DateTimeSerializer implements JsonSerializer<Date> {

        @Override
        public JsonElement serialize(Date date, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(new DateTime(date,DateTimeZones.UTC).toString(ISODateTimeFormat.dateTimeNoMillis()));
        }
    }
    
    private static final class JodaDateTimeSerializer implements JsonSerializer<DateTime> {

        @Override
        public JsonElement serialize(DateTime date, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(date.toString(ISODateTimeFormat.dateTimeNoMillis()));
        }
    }
}
