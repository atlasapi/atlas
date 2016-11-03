package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;
import org.jdom.Element;

import static com.google.common.base.Preconditions.checkNotNull;


public class C4AtomFeedUriExtractor implements C4UriExtractor<Feed, Feed, Entry> {

    private static final String DC_PROGRAMME_ID = "dc:relation.programmeId";
    private static final String FEED_ID_PREFIX_PATTERN = "tag:[a-z0-9.]+\\.channel4\\.com,\\d{4}:/programmes/";
    private static final Pattern SERIES_PAGE_ID_PATTERN = Pattern.compile(
            String.format(
                    "%s(%s/episode-guide/series-\\d+)",
                    FEED_ID_PREFIX_PATTERN,
                    C4AtomApi.WEB_SAFE_NAME_PATTERN
            ));
    private final C4LinkBrandNameExtractor linkExtractor = new C4LinkBrandNameExtractor();

    private static final Splitter URL_SPLITTER = Splitter.on('/');
    private static final Pattern OD_PATTERN = Pattern.compile("^\\d+-\\d+$");

    @Override
    @SuppressWarnings("unchecked")
    public Optional<String> uriForBrand(Publisher publisher, Feed feed) {
        for (Object link : Iterables.concat(feed.getAlternateLinks(), feed.getOtherLinks())) {
            Optional<String> uri = linkExtractor.atlasBrandUriFrom(publisher, ((Link)link).getHref());
            if (uri.isPresent()) {
                return Optional.of(uri.get());
            }
        }
        return Optional.absent();
    }

    @Override
    public Optional<String> uriForSeries(Publisher publisher, Feed feed) {
        Matcher matcher = SERIES_PAGE_ID_PATTERN.matcher(feed.getId());
        if (matcher.matches()) {
            return Optional.of(String.format("http://%s/pmlsd/%s", publisherHost(publisher), matcher.group(1)));
        }
        return Optional.absent();
    }

    @Override
    public Optional<String> uriForItem(Publisher publisher, Entry entry) {
        String progId = C4AtomApi.foreignElementLookup(entry).get(DC_PROGRAMME_ID);
        checkNotNull(progId, "No programmeId in entry: %s", entry.getId());
        return Optional.of(String.format("http://%s/pmlsd/%s", publisherHost(publisher), progId));
    }

    @SuppressWarnings("Guava")
    @Nullable
    @Override
    public Optional<String> uriForClip(Publisher publisher, Entry entry) {

        List links = (List<Link>) Stream.concat(
                entry.getOtherLinks().stream(),
                entry.getAlternateLinks().stream())
                .collect(Collectors.toList());

        java.util.Optional<String> odLink = ((List<Link>) links).stream()
                .map(Link::getHref)
                .filter(Objects::nonNull)
                .filter(this::is4OdLink)
                .findFirst();

        if (odLink.isPresent()) {
            return Optional.of(odLink.get());
        }

        Element mediaGroup = C4AtomApi.mediaGroup(entry);
        if (mediaGroup == null) {
            return null;
        }

        Element player = mediaGroup.getChild("player", C4AtomApi.NS_MEDIA_RSS);
        if (player == null) {
            return null;
        }

        return Optional.of(player.getAttributeValue("url"));
    }

    private boolean is4OdLink(String href) {
        List<String> parts = URL_SPLITTER.splitToList(href);

        if (parts.size() < 2) {
            return false;
        }

        parts = Lists.reverse(parts);

        if (!"on-demand".equals(parts.get(1))) {
            return false;
        }

        String seriesAndEpisodeNumber = parts.get(0);
        return OD_PATTERN.matcher(seriesAndEpisodeNumber).matches();
    }

    private String publisherHost(Publisher publisher) {
        String host = C4PmlsdModule.PUBLISHER_TO_CANONICAL_URI_HOST_MAP.get(publisher);
        if (host == null) {
            throw new IllegalArgumentException("Could not map publisher " + publisher.key() + " to a canonical URI host");
        }
        return host;
    }
}
