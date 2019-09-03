package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

class C4ContentLinker {
    
    private static final ModelWithPayload<Series> PLACEHOLDER = new ModelWithPayload<>(new Series(), null);
    private static final CharSequence PMLSC_URI_PREFIX = "http://pmlsc.channel4.com/pmlsd/";
    private static final CharSequence TAG_URI_PREFIX = "tag:pmlsc.channel4.com,2009:/programmes/";

    public SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> link4odToEpg(SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> episodeGuide, List<ModelWithPayload<Episode>> fourOd) {

        Map<String, ModelWithPayload<Episode>> odIndex = Maps.newHashMap(
                Maps.uniqueIndex(
                        fourOd,
                        modelWithPayload -> modelWithPayload.getModel().getCanonicalUri())
        );

        for (ModelWithPayload<Episode> episodeWithPayload : episodeGuide.values()) {
            Episode episode = episodeWithPayload.getModel();
            ModelWithPayload<Episode> odEpisode = odIndex.remove(episode.getCanonicalUri());
            if (odEpisode != null) {
                episode.setVersions(odEpisode.getModel().getVersions());
                episode.addAliasUrls(odEpisode.getModel().getAliasUrls());
            }
        }
        
        for (ModelWithPayload<Episode> episode : odIndex.values()) {
            episodeGuide.put(PLACEHOLDER, episode);
        }

        return episodeGuide;
    }

    public SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> populateBroadcasts(SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> episodeGuideContent, List<Episode> epgContent) {

        Multimap<String, Episode> indexedEpg = Multimaps.index(epgContent, TO_TAG_URI);

        for (ModelWithPayload<Episode> episode : episodeGuideContent.values()) {
            for (String alias : episode.getModel().getAliasUrls()) {
                for (Episode broadcastEpisode : indexedEpg.get(alias)) {
                    Version version = episodeVersion(episode.getModel());
                    Broadcast broadcast = getOnlyBroadcast(broadcastEpisode);
                    addOrUpdateBroadcastInVersion(version, broadcast);
                }
            }
        }

        return episodeGuideContent;
    }

    private void addOrUpdateBroadcastInVersion(Version version, Broadcast broadcast) {
        boolean broadcastExists = false;
        for (Broadcast existingBroadcast : version.getBroadcasts()) {
            if (existingBroadcast.equals(broadcast)) {
                existingBroadcast.setAliases(broadcast.getAliases());
                existingBroadcast.setLastUpdated(broadcast.getLastUpdated());
                broadcastExists = true;
            }
        }
        if (!broadcastExists) {
            version.addBroadcast(broadcast);
        }
    }

    private Version episodeVersion(Episode episode) {
        Version version;
        if (episode.getVersions().isEmpty()) {
            version = new Version();
            episode.addVersion(version);
        } else {
            version = Iterables.getOnlyElement(episode.getVersions());
        }
        return version;
    }

    private Broadcast getOnlyBroadcast(Episode broadcastEpisode) {
        Version version = Iterables.getOnlyElement(broadcastEpisode.getVersions(), null);
        if (version == null) {
            return null;
        }
        return Iterables.getOnlyElement(version.getBroadcasts(), null);
    }

    public SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> linkClipsToContent(SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> content, List<Clip> clips, Brand brand) {

        Map<String, Content> lookup = toContentLookup(content);

        for (Clip clip : clips) {
            Content found = findContent(lookup, clip);
            if (found != null) {
                // This is a temporary container of the series/episode number until we have
                // episode IDs in the clip API.
                // 
                // The clipOf field should be unset, since it's not actually a reference to
                // another item
                clip.setClipOf(null);
                found.addClip(clip);
            } else {
                brand.addClip(clip);
            }
        }

        return content;
    }

    public static final Pattern SERIES_AND_EPISODE_NUMBER_IN_ANY_URI = Pattern.compile("series-(\\d+)/episode-(\\d+)");
    
    private Content findContent(Map<String, Content> lookup, Clip clip) {
        return lookup.get(clip.getClipOf());
    }

    private Map<String, Content> toContentLookup(SetMultimap<ModelWithPayload<Series>, ModelWithPayload<Episode>> content) {
        Map<String, Content> lookup = Maps.newHashMap();
        for (ModelWithPayload<? extends Content> c : Iterables.concat(content.keys(), content.values())) {
            Optional<String> key = createKey(c.getModel());
            if (key.isPresent()) {
                lookup.put(key.get(), c.getModel());
            }
        }
        return lookup;
    }

    private Optional<String> createKey(Identified ided) {
        if (ided instanceof Series) {
            Series series = (Series) ided;
            return createKeyForSeries(series.getSeriesNumber());
        }
        if (ided instanceof Episode) {
            Episode episode = (Episode) ided;
            return createKeyForEpisode(episode.getSeriesNumber(), episode.getEpisodeNumber());
        }
        if (ided instanceof Brand) {
            return Optional.of("brand");
        }
        throw new IllegalArgumentException("Unsupported type: " + ided.getClass().getCanonicalName());
    }
    
    public Optional<String> createKeyForSeries(Integer seriesNumber) {
        if (seriesNumber == null) {
            return Optional.empty();
        }
        return Optional.of(String.format("s%d", seriesNumber));
    }
    
    public Optional<String> createKeyForEpisode(Integer seriesNumber, Integer episodeNumber) {
        if (seriesNumber == null || episodeNumber == null) {
            return Optional.empty();
        }
        return Optional.of(String.format("s%d-e%d", seriesNumber, episodeNumber));
    }
    
    private static Function<Episode, String> TO_TAG_URI = episode ->
            episode.getCanonicalUri().replace(PMLSC_URI_PREFIX, TAG_URI_PREFIX);
}
