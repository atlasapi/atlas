package org.atlasapi.remotesite.btvod;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.List;

public class BtVodSubGenreParser {

    private static final Type RETURN_TYPE = new TypeToken<List<String>>(){}.getType();

    private final Gson gson = new Gson();

    public List<String> parse(@Nullable String subGenreString) {
        if (Strings.isNullOrEmpty(subGenreString)) {
            return ImmutableList.of();
        }

        return gson.fromJson(subGenreString, RETURN_TYPE);
    }
}
