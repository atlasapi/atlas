package org.atlasapi.equiv;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;


public class EquivalenceBreaker {

    private final LookupEntryStore entryStore;
    private final LookupWriter lookupWriter;
    private final ContentResolver contentResolver;
    
    private EquivalenceBreaker(
            ContentResolver contentResolver,
            LookupEntryStore entryStore,
            LookupWriter lookupWriter
    ) {
        this.entryStore = checkNotNull(entryStore);
        this.lookupWriter = checkNotNull(lookupWriter);
        this.contentResolver = checkNotNull(contentResolver);
    }

    public static EquivalenceBreaker create(
            ContentResolver contentResolver,
            LookupEntryStore entryStore,
            LookupWriter lookupWriter
    ) {
        return new EquivalenceBreaker(contentResolver, entryStore, lookupWriter);
    }
    
    public void removeFromSet(String sourceUri, final String directEquivUriToRemove) {
        Maybe<Identified> possibleSource = 
                contentResolver.findByCanonicalUris(ImmutableSet.of(sourceUri))
                               .getFirstValue();
        
        if (!possibleSource.hasValue()) {
            throw new IllegalArgumentException("Invalid source URI: " + sourceUri);
        }
        
        Described source = (Described) possibleSource.requireValue();
        LookupEntry lookupEntry = 
                Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableSet.of(sourceUri)));
        
        if (!ImmutableSet.copyOf(Iterables.transform(lookupEntry.directEquivalents(), LookupRef.TO_URI))
                .contains(directEquivUriToRemove)) {
            throw new IllegalArgumentException("Direct equivalence to " 
                            + directEquivUriToRemove + " not found");
        }
        
        Iterable<LookupRef> filteredRefs = Iterables.filter(lookupEntry.directEquivalents(), 
                new Predicate<LookupRef>() {

                    @Override
                    public boolean apply(LookupRef input) {
                        return !input.uri().equals(directEquivUriToRemove);
                    }
                });
        
        Iterable<Described> equivalents = Iterables.filter(
                contentResolver
                    .findByCanonicalUris(Iterables.transform(filteredRefs, LookupRef.TO_URI))
                    .getAllResolvedResults(), Described.class);
        
        lookupWriter.writeLookup(ContentRef.valueOf(source), 
                Iterables.transform(equivalents, ContentRef.FROM_CONTENT), Publisher.all());
    }
}
