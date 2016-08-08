package org.atlasapi.input;

import java.io.IOException;
import java.io.Reader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class GsonModelReader implements ModelReader {

    private final Gson gson;

    public GsonModelReader(GsonBuilder builder) {
        this(builder.create());
    }

    public GsonModelReader(Gson gson) {
        this.gson = gson;
    }

    @Override
    public <T> T read(Reader reader, Class<T> cls, Boolean strict) throws IOException, ReadException{
        try {
            return gson.fromJson(reader, cls);
        } catch (JsonSyntaxException jse) {
            throw new ReadException(jse);
        } catch (JsonIOException ioe) {
            throw new IOException(ioe);
        }
    }

}