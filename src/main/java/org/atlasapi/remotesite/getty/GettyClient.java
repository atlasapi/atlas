package org.atlasapi.remotesite.getty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.common.io.CharStreams;
import com.google.gson.JsonElement;

public class GettyClient {

    private static final String EXPIRED_TOKEN_CODE = "AUTH-012";

    private final GettyTokenFetcher tokenFetcher;

    private String token;

    public GettyClient(GettyTokenFetcher tokenFetcher) {
        this.tokenFetcher = checkNotNull(tokenFetcher);
    }

    public String getVideoResponse(Collection<String> gettyResourceIds) throws ClientProtocolException, IOException {
        this.token = tokenFetcher.getToken();
        String response = getResponse(token, gettyResourceIds);
        if (response.contains(EXPIRED_TOKEN_CODE)) {  // TODO handle this properly
            this.token = tokenFetcher.getToken();
            return getResponse(token, gettyResourceIds);
        }
        return response;
    }

    private String getResponse(String token, Collection<String> gettyResourceIds) throws IOException {
        HttpPost post = new HttpPost("https://connect.gettyimages.com/v1/search/SearchForVideos");
        post.setHeader("Content-type", "application/json");

        VideoRequest videoRequest = new VideoRequest(token, gettyResourceIds);
        JsonElement json = videoRequest.toJson();
        post.setEntity(new StringEntity(json.toString()));

        HttpResponse resp = new DefaultHttpClient().execute(post);
        InputStreamReader reader = new InputStreamReader(resp.getEntity().getContent());
        return CharStreams.toString(reader);
    }

}
