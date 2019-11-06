package org.atlasapi.equiv;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkNotNull;


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

    /**
     * Keep in mind this is very inefficient, as if you are removing everything, you are calling
     * this multiple times, say e.g. you are unpublishing something and nuking the whole equiv set,
     * you will be re-resolving, recalculating and re-saving the transitive sets of
     * everything in the set all the time.
     * @param sourceUri The uri of the item, that we want to remove the equivalence from
     * @param directEquivUriToRemove The equivalence to remove from the source item
     */
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

        if (!lookupEntry.directEquivalents()
                .stream()
                .map(LookupRef.TO_URI::apply)
                .collect(Collectors.toList())
                .contains(directEquivUriToRemove)) {
            throw new IllegalArgumentException("Direct equivalence to " 
                            + directEquivUriToRemove + " not found");
        }
        
        Iterable<LookupRef> filteredRefs = lookupEntry.directEquivalents()
                .stream()
                .filter(input -> !input.uri().equals(directEquivUriToRemove))
                .collect(Collectors.toList());
        
        Iterable<ContentRef> equivalents = contentResolver
                .findByCanonicalUris(StreamSupport.stream(filteredRefs.spliterator(), false)
                        .map(LookupRef.TO_URI::apply)
                        .collect(Collectors.toList()))
                .getAllResolvedResults()
                .stream()
                .filter(Described.class::isInstance)
                .map(Described.class::cast)
                .map(ContentRef.FROM_CONTENT::apply)
                .collect(Collectors.toList());
        
        lookupWriter.writeLookup(ContentRef.valueOf(source), equivalents, Publisher.all());
    }

    /**
     *
     * @param source The item we want to remove equivalences from
     * @param sourceLE The lookup record of the source item
     * @param directEquivUrisToRemove The direct equivalence uris we want to remove.
     *                                If source uri is included, this method will ignore it.
     * @param explicitEquivUrisToRemove The explicit equivalence uris we want to remove.
     *                                If source uri is included, this method will ignore it.
     */
    public void removeFromSet(Described source,
            LookupEntry sourceLE,
            Set<String> directEquivUrisToRemove,
            Set<String> explicitEquivUrisToRemove
    ){
        //make sure to not remove yourself.
        directEquivUrisToRemove =
                Sets.difference(directEquivUrisToRemove, ImmutableSet.of(source.getCanonicalUri()));

        Set<String> existingDirectEquivUris = sourceLE.directEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(Collectors.toSet());

        Sets.SetView<String> remainingDirectEquivalences =
                Sets.difference(existingDirectEquivUris, directEquivUrisToRemove);

        ResolvedContent resolvedDirectEquivList =
                contentResolver.findByCanonicalUris(remainingDirectEquivalences);
        List<ContentRef> newEquivs = ImmutableList.of();
        if (resolvedDirectEquivList != null) {
            newEquivs = resolvedDirectEquivList
                    .getAllResolvedResults()
                    .stream()
                    .filter(Described.class::isInstance)
                    .map(Described.class::cast)
                    .map(ContentRef.FROM_CONTENT::apply)
                    .collect(Collectors.toList());
        }

        if(!explicitEquivUrisToRemove.isEmpty()){
            explicitEquivUrisToRemove =
                    Sets.difference(explicitEquivUrisToRemove, ImmutableSet.of(source.getCanonicalUri()));
            Set<String> existingExplicitEquivUris = sourceLE.explicitEquivalents()
                    .stream()
                    .map(LookupRef::uri)
                    .collect(Collectors.toSet());
            Sets.SetView<String> remainingExplicitEquivUris =
                    Sets.difference(existingExplicitEquivUris, explicitEquivUrisToRemove);
            ResolvedContent resolvedExplicitEquivList =
                    contentResolver.findByCanonicalUris(remainingExplicitEquivUris);
            if (resolvedExplicitEquivList != null) {
                newEquivs.addAll(resolvedExplicitEquivList.getAllResolvedResults()
                        .stream()
                        .filter(Described.class::isInstance)
                        .map(Described.class::cast)
                        .map(ContentRef.FROM_CONTENT::apply)
                        .collect(Collectors.toList()));
            }
        }

        lookupWriter.writeLookup(ContentRef.valueOf(source), newEquivs, Publisher.all());
    }

    public void removeFromSet(Described source,
            LookupEntry sourceLE,
            Set<String> directEquivUrisToRemove
    ){
        removeFromSet(source, sourceLE, directEquivUrisToRemove, Sets.newHashSet());
    }
}
