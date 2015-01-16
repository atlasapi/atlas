package org.atlasapi.remotesite.getty;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.io.CharStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GettyTokenFetcher {

    private static final String GETTY_OAUTH_URL = "https://connect.gettyimages.com/oauth2/token"; 
    private static final String ACCESS_TOKEN_KEY = "access_token";

    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;

    public GettyTokenFetcher(String clientId, String clientSecret, String username, String password) {
        this.clientId = checkNotNull(clientId);
        this.clientSecret = checkNotNull(clientSecret);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
    }

    public String getToken() throws ClientProtocolException, IOException {
        String oauth = oauth();
        return parseToken(oauth);
    }

    private String oauth() throws ClientProtocolException, IOException {
        HttpPost post = new HttpPost(GETTY_OAUTH_URL);

        Builder<BasicNameValuePair> params = new ImmutableList.Builder<BasicNameValuePair>();
        params.add(new BasicNameValuePair("grant_type", "password"));
        params.add(new BasicNameValuePair("client_id", clientId));
        params.add(new BasicNameValuePair("client_secret", clientSecret));
        params.add(new BasicNameValuePair("username", username));
        params.add(new BasicNameValuePair("password", password));
        // This could, if necessary, hold a refresh token instead of the username and password.
        // The refresh token would, however, expire after a year, as well as if the password changed.
        // See https://github.com/gettyimages/connect/blob/master/oauth2.md

        post.setEntity(new UrlEncodedFormEntity(params.build()));

        HttpResponse resp = new DefaultHttpClient().execute(post);
        InputStreamReader reader = new InputStreamReader(resp.getEntity().getContent());
        return CharStreams.toString(reader);
    }

    private String parseToken(String content) {
        JsonObject parse = (JsonObject) new JsonParser().parse(content);
        JsonElement element = parse.get(ACCESS_TOKEN_KEY);
        if (element == null) {
            throw new RuntimeException("No token found in response:\n" + content);
        }
        return element.getAsString();
    }

}
