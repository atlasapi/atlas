package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.remotesite.support.atom.AtomClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.url.Urls;
import com.sun.syndication.feed.atom.Feed;

import java.util.Optional;

public class C4AtomApiClient {
    
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomClient client;
    private final String apiUrlBase;
    private final Optional<String> platform;

    private final C4LinkBrandNameExtractor linkExtractor = new C4LinkBrandNameExtractor();

    public C4AtomApiClient(SimpleHttpClient client, String apiUrlBase, Optional<String> platform) {
        this.client = new AtomClient(client);
        this.apiUrlBase = apiUrlBase;
        this.platform = platform;
    }
    
    public Optional<Feed> brandFeed(String canonicalBrandUri) {
        return getFeed(canonicalBrandUri, "");
    }

    public Optional<Feed> brand4oDFeed(String canonicalBrandUri) {
        return getFeed(canonicalBrandUri, "/4od");
    }
    
    public Optional<Feed> brandEpisodeGuideFeed(String canonicalBrandUri) {
        return getFeed(canonicalBrandUri, "/episode-guide");
    }
    
    public Optional<Feed> seriesEpisodeGuideFeed(String canonicalBrandUri, int seriesNumber) {
        return getFeed(canonicalBrandUri, "/episode-guide/series-" + seriesNumber);
    }

    public Optional<Feed> brandEpgFeed(String canonicalBrandUri) {
        return getFeed(canonicalBrandUri, "/epg");
    }

    public Optional<Feed> brandVideoFeed(String canonicalBrandUri) {
        return getFeed(canonicalBrandUri, "/videos/all");
    }

    private Optional<Feed> getFeed(String brandUri, String suffix) {
        String baseUri = String.format("%s%s%s.atom", apiUrlBase, brandName(brandUri), suffix);
        return get(optionallyAppendPlatform(baseUri));
    }
    
    private String brandName(String canonicalBrandUri) {
        return linkExtractor.brandNameFrom(canonicalBrandUri).get();
    }
    
    private Optional<Feed> get(String uri) {
        Feed feed = null;
        try {
            log.debug("Fetching {}", uri);
            feed = client.get(uri);
        } catch (HttpException e) {
            if (HttpStatusCode.NOT_FOUND.code() == e.getResponse().statusCode()
                    // C4 have started returning 400 instead of 404. We have asked them
                    // to change back but in the meantime, we'll treat 400s as content not found.
                    || HttpStatusCode.BAD_REQUEST.code() == e.getResponse().statusCode()) {
                log.warn(uri + " not found");
            } else {
                log.error(e.getResponse().statusCode() + " exception fetching " + uri, e);
            }
        } catch (Exception e) {
            log.error("Exception fetching " + uri, e);
        }
        return Optional.ofNullable(feed);
    }
    
    private String appendPlatform(String url) {
        return Urls.appendParameters(url, "platform", platform.get());
    }
    
    private String optionallyAppendPlatform(String url) {
        return platform.isPresent() ? appendPlatform(url) : url;
    }

}
