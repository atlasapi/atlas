package org.atlasapi.equiv.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;

import java.util.Set;

public class LookupWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {
 
    private final LookupWriter writer;
    private final Set<Publisher> publishers;

    @VisibleForTesting
    LookupWritingEquivalenceHandler(
            LookupWriter writer,
            Iterable<Publisher> publishers
    ) {
        this.writer = writer;
        this.publishers = ImmutableSet.copyOf(publishers);
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
                Publisher.all()
        );
    }

    @Override
    public boolean handle(EquivalenceResults<T> results) {

        Optional<Set<LookupEntry>> writtenLookups = writer.writeLookup(
                ContentRef.valueOf(results.subject()),
                Iterables.transform(results.strongEquivalences(), ContentRef.FROM_CONTENT),
                publishers
        );

        return writtenLookups.isPresent();
    }
}
