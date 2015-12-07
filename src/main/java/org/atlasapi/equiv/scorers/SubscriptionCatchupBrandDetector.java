package org.atlasapi.equiv.scorers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Guess whether a brand contains a few episodes matching a subscription
 * catch-up model, where the most recent few broadcast episodes are 
 * available to watch, but no more. This is useful for a VOD catalogue
 * that doesn't contain historical episodes, but just those currently
 * available.
 *
 */
public class SubscriptionCatchupBrandDetector {

    private final ContentResolver contentResolver;

    public SubscriptionCatchupBrandDetector(ContentResolver contentResolver) {
        this.contentResolver = checkNotNull(contentResolver);
    }
    
    public boolean couldBeSubscriptionCatchup(Container subject, List<Series> series) {
        if (series.size() > 2
                || series.size() == 0) {
            return false;
        }
        if (series.size() == 2
                && !areSeriesConsecutive(series.get(0), series.get(1))) {
            return false;
        }
        for (Series s : series ) {
            if (!allEpisodesAreConsecutive(s)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean areSeriesConsecutive(Series s1, Series s2) {
        return !(s1.getSeriesNumber() == null 
                    || s2.getSeriesNumber() == null
                    || Math.abs(s1.getSeriesNumber() - s2.getSeriesNumber()) > 1);
    }
    
    private boolean allEpisodesAreConsecutive(Series s) {
        ResolvedContent children = contentResolver.findByCanonicalUris(ImmutableList.copyOf(Iterables.transform(s.getChildRefs(), ChildRef.TO_URI)));
        Iterable<Episode> episodes = Iterables.filter(children.getAllResolvedResults(), Episode.class);
        List<Integer> episodeNumbers = Lists.newArrayList(
                                           Iterables.filter(
                                                Iterables.transform(episodes, TO_EPISODE_NUMBER), 
                                                Predicates.notNull()
                                           )
                                       );
        
        Collections.sort(Lists.newArrayList(episodeNumbers));
        if (episodeNumbers.size() != s.getChildRefs().size()) {
            // If null episode numbers were removed
            return false;
        }
        Integer previousEpisodeNumber = null;
        for (Integer episodeNumber : episodeNumbers) {
            if (previousEpisodeNumber != null
                    && episodeNumber - previousEpisodeNumber > 1) {
                return false;
            }
            previousEpisodeNumber = episodeNumber;
        }
        return true;
    }
    
    private static Function<Episode, Integer> TO_EPISODE_NUMBER = new Function<Episode, Integer>() {
        
        @Override
        public Integer apply(Episode input) {
            return input.getEpisodeNumber();
        }
    };
}
