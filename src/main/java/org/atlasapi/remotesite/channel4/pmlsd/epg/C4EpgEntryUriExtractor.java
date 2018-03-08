package org.atlasapi.remotesite.channel4.pmlsd.epg;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.remotesite.channel4.pmlsd.C4PmlsdModule;
import org.atlasapi.remotesite.channel4.pmlsd.C4UriExtractor;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class C4EpgEntryUriExtractor implements C4UriExtractor<C4EpgEntry, C4EpgEntry, C4EpgEntry>{
    
    private final Pattern uriPattern = Pattern.compile(
        "https?://(.+).channel4.com/([^/]+)/([^./]+)(.atom|/4od.atom|/episode-guide/series-(\\d+)(.atom|/episode-(\\d+).atom))"
    );
    private static final String ATLAS_URI_FORMAT = "http://%s/pmlsd/%s";
    
    private static final int SERIES_NUMBER_GROUP = 5;
    private static final int BRAND_NAME_GROUP = 3;
    
    private static final String SERIES_URI_INFIX = "/episode-guide/series-";

    @Override
    public Optional<C4UriAndAliases> uriForBrand(Publisher publisher, C4EpgEntry entry){
        if (!entry.hasRelatedLink()) {
            return Optional.empty();
        }
        String linkUri = entry.getRelatedLink();
        Matcher matcher = uriPattern.matcher(linkUri);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(C4UriAndAliases.create(
                String.format(ATLAS_URI_FORMAT, publisherHost(publisher), matcher.group(3))
        ));
    }
    
    @Override
    public Optional<C4UriAndAliases> uriForSeries(Publisher publisher, C4EpgEntry entry){
        if (!entry.hasRelatedLink()) {
            return Optional.empty();
        }
        String linkUri = entry.getRelatedLink();
        Matcher matcher = uriPattern.matcher(linkUri);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        String seriesNumber = matcher.group(SERIES_NUMBER_GROUP);
        if (seriesNumber == null) {
            return Optional.empty();
        }
        return Optional.of(C4UriAndAliases.create(
                String.format(
                        ATLAS_URI_FORMAT,
                        publisherHost(publisher),
                        matcher.group(BRAND_NAME_GROUP) + SERIES_URI_INFIX + seriesNumber
                )
        ));
    }
    
    @Override
    public Optional<C4UriAndAliases> uriForItem(Publisher publisher, C4EpgEntry entry){
        return Optional.of(C4UriAndAliases.create(
                String.format(ATLAS_URI_FORMAT, publisherHost(publisher), entry.programmeId()),
                new Alias(C4AtomApi.ALIAS, entry.programmeId()),
                new Alias(C4AtomApi.ALIAS_FOR_BARB, entry.programmeId())
        ));
    }

    @Override
    public Optional<C4UriAndAliases> uriForClip(Publisher publisher, C4EpgEntry remote) {
        throw new UnsupportedOperationException("Clips not supported as not ingested from EPG");
    }
    
    private String publisherHost(Publisher publisher) {
        String host = C4PmlsdModule.PUBLISHER_TO_CANONICAL_URI_HOST_MAP.get(publisher);
        if (host == null) {
            throw new IllegalArgumentException("Could not map publisher " + publisher.key() + " to a canonical URI host");
        }
        return host;
    }

}
