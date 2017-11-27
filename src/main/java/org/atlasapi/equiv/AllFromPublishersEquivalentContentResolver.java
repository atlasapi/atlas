package org.atlasapi.equiv;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

/**
 * This class slightly alters the logic of the default equivalentContentResolver, but allowing
 * multiple items of the same publisher to be returned.
 * <p>
 * At the moment of writing it was not clear whether this was an intended feature of the default
 * implementation. The requirement for this class stems from the new logic for amazon ingests, where
 * multiple versions of the same item are written in the db and then equived together (as opposed to
 * the previous implementation, where similar items where deduped in a single item with versions).
 * Consequently, before these items are uploaded to YV, we need to merge them back to 1, but the
 * default equivContentResolver does not return all the equived items from the same publisher.
 */
public class AllFromPublishersEquivalentContentResolver extends DefaultEquivalentContentResolver{

    public AllFromPublishersEquivalentContentResolver(
            KnownTypeContentResolver contentResolver,
            LookupEntryStore lookupResolver) {
        super(contentResolver, lookupResolver);
    }

    protected ImmutableSetMultimap<LookupEntry, LookupRef> resolveAndFilter(
            SetMultimap<LookupRef, LookupRef> secondaryResolve,
            ImmutableSetMultimap<LookupEntry, LookupRef> subjsToEquivs,
            Predicate<LookupRef> sourceFilter
    ) {
        return subjsToEquivs;
    }

}
