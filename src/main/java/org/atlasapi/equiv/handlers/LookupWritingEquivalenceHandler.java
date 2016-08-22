package org.atlasapi.equiv.handlers;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LookupWritingEquivalenceHandler<T extends Content> implements EquivalenceResultHandler<T> {
 
    private final LookupWriter writer;
    private final LoadingCache<String, String> seenAsEquiv;
    private final Set<Publisher> publishers;
    
    public LookupWritingEquivalenceHandler(LookupWriter writer, Iterable<Publisher> publishers) {
        this(writer, publishers, Duration.standardHours(5));
    }

    public LookupWritingEquivalenceHandler(LookupWriter writer, Iterable<Publisher> publishers, Duration cacheDuration) {
        this.writer = writer;
        this.publishers = ImmutableSet.copyOf(publishers);
        this.seenAsEquiv = CacheBuilder.newBuilder().expireAfterWrite(cacheDuration.getMillis(), MILLISECONDS).build(new CacheLoader<String, String>() {
            @Override
            public String load(String key) throws Exception {
                return "";
            }
        });
    }
    
    @Override
    public void handle(
            EquivalenceResult<T> result,
            Optional<String> taskId
    ) {
        Iterable<T> equivs = Iterables.transform(result.strongEquivalences().values(),ScoredCandidate.<T>toCandidate());
        
        //abort writing if seens as equiv and not equiv to anything
        if(seenAsEquiv.asMap().containsKey(result.subject().getCanonicalUri()) && Iterables.isEmpty(equivs)) {
            return;
        }
        
        for (T equiv : equivs) {
            seenAsEquiv.getUnchecked(equiv.getCanonicalUri());
        }
        
        writer.writeLookup(ContentRef.valueOf(result.subject()), Iterables.transform(equivs, ContentRef.FROM_CONTENT), publishers);
    }

}
