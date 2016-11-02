package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.Clock;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

import static com.google.common.base.Preconditions.checkNotNull;

public class C4BrandClipExtractor extends C4MediaItemExtractor<Clip> {

    private final ContentFactory<Feed, Feed, Entry> contentFactory;
    private final C4AtomEntryVersionExtractor versionExtractor;
    private final C4AtomFeedUriExtractor uriExtractor = new C4AtomFeedUriExtractor();
    private final Publisher publisher;
    private final C4ContentLinker contentLinker;

    public C4BrandClipExtractor(ContentFactory<Feed, Feed, Entry> contentFactory, Publisher publisher, 
            C4LocationPolicyIds locationPolicyIds, C4ContentLinker contentLinker, Clock clock) {
        super(clock);
        this.contentFactory = contentFactory;
        this.publisher = publisher;
        // TODO: Do we have platform-specific clips?
        versionExtractor = new C4AtomEntryVersionExtractor(Optional.<Platform>absent(), locationPolicyIds, false);
        this.contentLinker = checkNotNull(contentLinker);
    }

    @Override
    protected Clip createItem(Entry entry, Map<String, String> lookup) {
        return contentFactory.createClip(entry).get();
    }
    
    @Override
    protected Clip setAdditionalItemFields(Entry entry, Map<String, String> lookup, Clip clip) {
        String fourOdUri = C4AtomApi.fourOdUri(entry);

        if (fourOdUri != null) {
            clip.addAliasUrl(fourOdUri);
        }

        clip.setIsLongForm(false);
        clip.setClipOf(possibleSeriesAndEpisodeNumberFrom(lookup).orNull());

        String uri = uriExtractor.uriForClip(publisher, entry).get();
        checkNotNull(uri, "No version URI extracted for %s", entry.getId());

        clip.addVersion(versionExtractor.extract(
                new C4VersionData(
                        entry.getId(),
                        uri,
                        getMedia(entry),
                        lookup,
                        clip.getLastUpdated()
                )
        ));

        return clip;
    }

    private Optional<String> possibleSeriesAndEpisodeNumberFrom(Map<String, String> lookup) {
        Integer episode = Ints.tryParse(Strings.nullToEmpty(lookup.get(C4AtomApi.DC_EPISODE_NUMBER)));
        Integer series = Ints.tryParse(Strings.nullToEmpty(lookup.get(C4AtomApi.DC_SERIES_NUMBER)));
        
        if (episode == null && series == null) {
            return Optional.absent();
        }
        
        if (episode == null && series != null) {
            return contentLinker.createKeyForSeries(series);
        }
        
        return contentLinker.createKeyForEpisode(series, episode);
    }
    
}
