package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Publisher;

import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;

/**
 * A {@link C4UriExtractor} that bases a canonical URI on the hierarchy URI. 
 * This URI is not intended to be used in items persisted in Atlas; it's intended for items
 * that are transient, used to stitch data together between different C4 ATOM feed.
 *  
 * This is a hack as programmeIds aren't available on brand epg ATOM feeds.
 *
 */
class C4BrandEpgUriExtractor implements C4UriExtractor<Feed, Feed, Entry> {
    
    private static final Pattern CANONICAL_EPISODE_URI_PATTERN 
        = Pattern.compile(String.format("%s%s/episode-guide/series-\\d+/episode-\\d+", 
                Pattern.quote(C4AtomApi.PROGRAMMES_BASE), C4AtomApi.WEB_SAFE_NAME_PATTERN));

    private static final String API_PATTERN_ROOT = "https?://[^.]*.channel4.com/pmlsd/";
    private static final Pattern EPISODE_API_PAGE_PATTERN 
        = Pattern.compile(String.format("%s(%s)/episode-guide/series-(\\d+)/episode-(\\d+).atom.*",  
                API_PATTERN_ROOT, C4AtomApi.WEB_SAFE_NAME_PATTERN));
    
    private static final Pattern WEB_EPISODE_URI_PATTERN 
        = Pattern.compile(String.format("%s(%s)/episode-guide/series-(\\d+)/episode-(\\d+).*",
                Pattern.quote(C4AtomApi.WEB_BASE), C4AtomApi.WEB_SAFE_NAME_PATTERN));

    
    @Override
    public Optional<C4UriAndAliases> uriForBrand(Publisher publisher, Feed remote) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<C4UriAndAliases> uriForSeries(Publisher publisher, Feed remote) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<C4UriAndAliases> uriForItem(Publisher publisher, Entry entry) {
        List<Link> links = entry.getAlternateLinks();
        
        for (Link link : links) {
            String href = link.getHref();
            if (isACanonicalEpisodeUri(href)) {
                return Optional.of(C4UriAndAliases.create(href));
            }
        }
        
        links = entry.getOtherLinks();
        for (Link link : links) {
            String href = link.getHref();
            if (isACanonicalEpisodeUri(href)) {
                return Optional.of(C4UriAndAliases.create(href));
            }
            Matcher matcher = EPISODE_API_PAGE_PATTERN.matcher(href);
            if(matcher.matches()) {
                return Optional.of(C4UriAndAliases.create(
                        episodeUri(
                                matcher.group(1),
                                Integer.parseInt(matcher.group(2)),
                                Integer.parseInt(matcher.group(3))
                        )
                ));
            }
            matcher = WEB_EPISODE_URI_PATTERN.matcher(href);
            if(matcher.matches()) {
                return Optional.of(C4UriAndAliases.create(
                        episodeUri(
                                matcher.group(1),
                                Integer.parseInt(matcher.group(2)),
                                Integer.parseInt(matcher.group(3))
                        )
                ));
            }
        }
        
        return Optional.empty();
    }

    @Override
    public Optional<C4UriAndAliases> uriForClip(Publisher publisher, Entry remote) {
        throw new UnsupportedOperationException();
    }
    
    private boolean isACanonicalEpisodeUri(String href) {
        return CANONICAL_EPISODE_URI_PATTERN.matcher(href).matches();
    }
    
    private static String seriesUriFor(String webSafeBrandName, int seriesNumber) {
        return C4AtomApi.PROGRAMMES_BASE + webSafeBrandName + "/episode-guide/series-" + seriesNumber;
    }

    private static String episodeUri(String webSafeBrandName, int seriesNumber, int episodeNumber) {
        return seriesUriFor(webSafeBrandName, seriesNumber) + "/episode-" + episodeNumber;
    }
}