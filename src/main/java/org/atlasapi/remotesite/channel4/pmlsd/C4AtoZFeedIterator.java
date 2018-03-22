package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.persistence.system.AToZUriSource;
import org.atlasapi.remotesite.support.atom.AtomClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.url.Urls;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;

import javax.annotation.Nullable;

public class C4AtoZFeedIterator extends AbstractIterator<Optional<Feed>> {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private static final Pattern PAGE_PATTERN = Pattern.compile("https?://[^.]*\\.channel4\\.com/[^/]*/atoz/(.+/page-\\d+).atom.*");

    private final AtomClient client;
    private final String uriBase;
    private final Optional<String> platform;
    private final Iterator<String> letterIterator;
    
    private String nextPageUri = null;

    public C4AtoZFeedIterator(SimpleHttpClient client, String uriBase, Optional<String> platform) {
        this.client = new AtomClient(client);
        this.uriBase = uriBase;
        this.platform = platform;
        this.letterIterator = ImmutableList.copyOf(new AToZUriSource("", "", true)).reverse().iterator();
    }
    
    @Override
    protected Optional<Feed> computeNext() {
        String nextUri = nextUri();
        if (nextUri == null) {
            return endOfData();
        }
        nextUri = optionallyAppendPlatform(nextUri);
        Feed feed = null;
        try {
            log.debug("Fetching {}", nextUri);
            feed = client.get(nextUri);
            nextPageUri = extractNextPageUri(feed);
        } catch (HttpException e) {
            log.warn(e.getResponse().statusCode() + ": Failed to fetch " + nextUri);
            nextPageUri = null;
        } catch (Exception e) {
            log.error("Failed to fetch " + nextUri, e);
            nextPageUri = null;
        }
        return Optional.ofNullable(feed);
    }

    private String optionallyAppendPlatform(String url) {
        return platform.isPresent() ? appendPlatform(url) : url;
    }
    
    private String appendPlatform(String url) {
        return Urls.appendParameters(url, "platform", platform.get());
    }

    @Nullable private String nextUri() {
        if (nextPageUri != null) {
            return constructUri(nextPageUri);
        } else if (letterIterator.hasNext()) {
            return constructUri(letterIterator.next());
        }
        return null;
    }

    private String constructUri(String letter) {
        return String.format("%satoz/%s.atom", uriBase, letter);
    }

    @Nullable private String extractNextPageUri(@Nullable Feed feed) {
        if (feed != null) {
            for (Object obj : feed.getOtherLinks()) {
                Link link = (Link) obj;
                if ("next".equals(link.getRel())) {
                    String next = link.getHref();
                    Matcher matcher = PAGE_PATTERN.matcher(next);
                    if (matcher.matches()) {
                        return matcher.group(1);
                    }
                }
            }
        }
        return null;
    }

}
