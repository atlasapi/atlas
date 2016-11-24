package org.atlasapi.equiv.handlers;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LookupWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {
 
    private final LookupWriter writer;
    private final LoadingCache<String, String> seenAsEquiv;
    private final Set<Publisher> publishers;

    @VisibleForTesting
    LookupWritingEquivalenceHandler(
            LookupWriter writer,
            Iterable<Publisher> publishers,
            Duration cacheDuration
    ) {
        this.writer = writer;
        this.publishers = ImmutableSet.copyOf(publishers);
        this.seenAsEquiv = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheDuration.getMillis(), MILLISECONDS)
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) throws Exception {
                        return "";
                    }
                });
    }

    /**
     * Create a new {@link LookupWritingEquivalenceHandler}.
     * <p>
     * This handler asserts direct equivalence or lack thereof on all publishers. This means all
     * outgoing equivalence edges need to be provided on every update and that the absence of an
     * equivalence assertion to any publisher will break any existing connections to that publisher.
     */
    public static <T extends Content> LookupWritingEquivalenceHandler<T> create(
            LookupWriter writer
    ) {
        return new LookupWritingEquivalenceHandler<>(
                writer,
                Publisher.all(),
                Duration.standardHours(5)
        );
    }

    @Override
    public void handle(EquivalenceResult<T> result) {
        Iterable<T> equivs = Iterables.transform(
                result.strongEquivalences().values(),
                ScoredCandidate.toCandidate()
        );
        
        // abort writing if seens as equiv and not equiv to anything
        if(seenAsEquiv.asMap().containsKey(result.subject().getCanonicalUri())
                && Iterables.isEmpty(equivs)) {
            return;
        }
        
        for (T equiv : equivs) {
            seenAsEquiv.getUnchecked(equiv.getCanonicalUri());
        }
        
        writer.writeLookup(
                ContentRef.valueOf(result.subject()),
                Iterables.transform(equivs, ContentRef.FROM_CONTENT),
                publishers
        );
    }
}
