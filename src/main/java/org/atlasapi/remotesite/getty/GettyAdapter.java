package org.atlasapi.remotesite.getty;

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GettyAdapter {

    private static final String SEARCH_RESULT = "SearchForVideosResult";
    private static final String VIDEOS = "Videos";
    private static final String EXPECTED_COUNT = "ItemTotalCount";

    private static final String ASSET_ID = "AssetId";
    private static final String DESCRIPTION = "Caption";
    private static final String DURATION = "ClipLength";
    private static final String TITLE = "Title";
    private static final String URLS = "Urls";
    private static final String THUMB = "Thumb";
    private static final String DATE_CREATED = "DateCreated";

    private static final String KEYWORDS = "Keywords";
    private static final String KEYWORD_TITLE = "Text";
    private static final String KEYWORD_TYPE = "Type";
    private static final ImmutableSet<String> KEYWORDS_TYPES_TO_IGNORE = ImmutableSet.of(
            "ImageTechnique",  // e.g. HD Format, Real Time
            "Composition",     // e.g. Medium Shot
            "NumberOfPeople",
            "Age",
            "Gender"
    );  // Keywords with these types are mostly facts about the video itself rather than concepts it relates to.

    private static final String ASPECT_RATIOS = "AspectRatios";

    public List<VideoResponse> parse(String text) {
        try {
            JsonObject parse = (JsonObject) new JsonParser().parse(text);
            JsonObject result = parse.get(SEARCH_RESULT).getAsJsonObject();

            JsonArray videoArray = result.get(VIDEOS).getAsJsonArray();
            Integer expectedCount = maybeInteger(result.get(EXPECTED_COUNT));

            Builder<VideoResponse> videos = new ImmutableList.Builder<VideoResponse>();
            //check here so that json parser doesn't fail
            if (videoArray.size() != 0) {
                for (JsonElement elem : videoArray) {
                    videos.add(createVideoResponse(elem.getAsJsonObject(), expectedCount));
                }
            }
            return videos.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse response:\n" + text, e);
        }
    }

    /** Gson is an enormous pile of crap and JsonElement.getAsString() just fails on 'JsonNull'. Helpful. */
    private String maybeString(JsonElement elem) {
        try {
            return elem.getAsString();
        } catch (Exception e) {
            return null;
        }
    }

    private Integer maybeInteger(JsonElement elem) {
        try {
            return elem.getAsInt();
        } catch (Exception e) {
            return null;
        }
    }

    private Iterable<JsonElement> maybeList(JsonElement elem) {
        try {
            return elem.getAsJsonArray();
        } catch (Exception e) {
            return ImmutableList.of();
        }
    }

    private VideoResponse createVideoResponse(JsonObject elem, Integer expectedCount) {
        VideoResponse videoResponse = new VideoResponse();

        videoResponse.setAssetId(maybeString(elem.get(ASSET_ID)));
        videoResponse.setDescription(maybeString(elem.get(DESCRIPTION)));
        videoResponse.setDuration(maybeString(elem.get(DURATION)));
        videoResponse.setKeywords(keywords(elem));
        videoResponse.setThumb(thumb(elem));
        videoResponse.setDateCreated(maybeString(elem.get(DATE_CREATED)));
        videoResponse.setTitle(maybeString(elem.get(TITLE)));
        videoResponse.setAspectRatios(aspectRatios(elem));
        videoResponse.setExpectedItemCount(expectedCount);

        return videoResponse;
    }

    private List<String> aspectRatios(JsonObject obj) {
        Builder<String> aspectRatios = new ImmutableList.Builder<String>();
        for (JsonElement elem : maybeList(obj.get(ASPECT_RATIOS))) {
            aspectRatios.add(elem.getAsString());
        }
        return aspectRatios.build();
    }

    private String thumb(JsonObject elem) {
        try {
            return elem.get(URLS).getAsJsonObject().get(THUMB).getAsString();
        } catch (Exception e) {
            return null;
        }
    }

    private List<String> keywords(JsonObject obj) {
        Builder<String> keywordStrings = new ImmutableList.Builder<String>();

        for (JsonElement elem : maybeList(obj.get(KEYWORDS))) {
            JsonObject kwObject = elem.getAsJsonObject();
            JsonElement maybeType = kwObject.get(KEYWORD_TYPE);
            if (maybeType != null && KEYWORDS_TYPES_TO_IGNORE.contains(maybeType.getAsString())) {
                continue;  // skip this keyword
            }
            String keywordTitle = Strings.emptyToNull(maybeString(kwObject.get(KEYWORD_TITLE)));
            if (keywordTitle != null) {
                keywordStrings.add(keywordTitle);
            }
        }

        return keywordStrings.build();
    }

}
