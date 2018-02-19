package org.atlasapi.remotesite.channel4.pirate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metabroadcast.common.security.UsernameAndPassword;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.channel4.pirate.model.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class C4PirateClient {

    private static final Logger log = LoggerFactory.getLogger(C4PirateClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PARAM_EPISODE_ID = "episodeID";

    private final HttpClient client;
    private final String c4PirateUri;
    private final UsernameAndPassword userAndPass;

    private C4PirateClient(HttpClient client, String c4PirateBaseUri, UsernameAndPassword userAndPass) {
        this.client = checkNotNull(client);
        this.c4PirateUri = format("%s?%s=", checkNotNull(c4PirateBaseUri), PARAM_EPISODE_ID);
        this.userAndPass = checkNotNull(userAndPass);
    }

    public static C4PirateClient create(String username, String password, String c4PirateBaseUri) {

        return new C4PirateClient(
                HttpClientBuilder.create()
                        .setUserAgent(HttpClients.ATLAS_USER_AGENT)
                        .build(),
                c4PirateBaseUri,
                new UsernameAndPassword(username, password)
        );
    }

    public List<Item> getItems(String episodeIds) throws IOException {
        validate(episodeIds);

        HttpGet getReq = new HttpGet(format("%s%s", c4PirateUri, episodeIds));
        getReq.addHeader(userAndPass.username(), userAndPass.password());
        getReq.addHeader(
                "Authorization",
                "Basic " + Base64.getUrlEncoder().encodeToString(String.format("%s:%s", userAndPass.username(), userAndPass.password()).getBytes())
        );

        log.info("PIRATE - sending request");
        HttpResponse response = client.execute(getReq);
        log.info("PIRATE - response received: {}", response.getStatusLine().getStatusCode());

        try {
            return MAPPER.readValue(response.getEntity().getContent(), MAPPER.getTypeFactory().constructCollectionType(List.class, Item.class));
           // return MAPPER.readValue(response.getEntity().getContent(), ItemsWrapper.class);
        } catch (Exception e) {
            log.error("Error reading response: {}", response, e);
            return null;
        }
    }

    private void validate(String episodeIds) {
        for (String episodeId : episodeIds.split(",")) {
            checkArgument(episodeId.contains("-"));
            checkArgument(episodeId.length() == 9);
        }
    }

}
