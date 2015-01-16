package org.atlasapi.remotesite.getty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class VideoRequest {

    private final String token;
    private Collection<String> gettyResourceIds;

    public VideoRequest(String token, Collection<String> gettyResourceIds) {
        this.token = checkNotNull(token);
        this.gettyResourceIds = checkNotNull(gettyResourceIds);
    }

    public JsonElement toJson() {
        JsonObject json = new JsonObject();

        JsonObject requestHeader = new JsonObject();
        requestHeader.addProperty("Token", token);

        JsonArray assetIds = new JsonArray();
        for (String id : gettyResourceIds) {
            assetIds.add(new JsonPrimitive(id));
        }
        JsonObject query = new JsonObject();
        query.add("AssetIds", assetIds);

        JsonObject resultOptions = new JsonObject();
        resultOptions.addProperty("ItemCount", gettyResourceIds.size());
        resultOptions.addProperty("ItemStartNumber", 1);

        JsonObject searchForVideosRequestBody = new JsonObject();
        searchForVideosRequestBody.add("Query", query);
        searchForVideosRequestBody.add("ResultOptions", resultOptions);

        json.add("RequestHeader", requestHeader);
        json.add("SearchForVideosRequestBody", searchForVideosRequestBody);

        return json;
    }

}
