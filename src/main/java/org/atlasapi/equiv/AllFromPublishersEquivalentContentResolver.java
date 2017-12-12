package org.atlasapi.equiv;

import java.util.Set;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.MorePredicates;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * This class slightly alters the logic of the default equivalentContentResolver, by allowing
 * multiple items of the same publisher to be returned, and multiple items from the same publisher
 * in the equivalence set.
 * <p>
 * The requirement for this class stems from the new logic for amazon ingests, where
 * multiple versions of the same item are written in the db and then equived together (as opposed to
 * the previous implementation, where similar items where deduped in a single item with versions).
 * Consequently, before these items are uploaded to YV, we need to merge them back to 1, but the
 * default equivContentResolver does not return all the equived items from the same publisher.
 *
 * Similarly, the repID service needs to be able to view the whole equiv set if it is to make an
 * educated choice of ID to represent the item.
 */
public class AllFromPublishersEquivalentContentResolver extends DefaultEquivalentContentResolver {

    public AllFromPublishersEquivalentContentResolver(
            KnownTypeContentResolver contentResolver,
            LookupEntryStore lookupResolver) {
        super(contentResolver, lookupResolver);
    }

    protected SetMultimap<LookupEntry, LookupRef> subjsToEquivs(
            Iterable<LookupEntry> resolved,
            Application application
    ) {
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(
                LookupRef.TO_SOURCE,
                Predicates.in(application.getConfiguration().getEnabledReadSources())
        );

        ImmutableSetMultimap.Builder<LookupEntry, LookupRef> subjsToEquivs = ImmutableSetMultimap.builder();
        for (LookupEntry entry : resolved) {
            Set<LookupRef> selectedEquivs = Sets.filter(entry.equivalents(), sourceFilter);
            subjsToEquivs.putAll(entry, selectedEquivs);
        }
        return subjsToEquivs.build();
    }

}
