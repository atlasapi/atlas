package org.atlasapi.equiv;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;


public class EquivalenceBreaker {

    private final LookupEntryStore entryStore;
    private final LookupWriter directLookupWriter;
    private final LookupWriter explicitLookupWriter;
    private final ContentResolver contentResolver;

    private EquivalenceBreaker(
            ContentResolver contentResolver,
            LookupEntryStore entryStore,
            LookupWriter directLookupWriter,
            LookupWriter explicitLookupWriter
    ) {
        this.entryStore = checkNotNull(entryStore);
        this.directLookupWriter = checkNotNull(directLookupWriter);
        this.contentResolver = checkNotNull(contentResolver);
        this.explicitLookupWriter = checkNotNull(explicitLookupWriter);
    }

    public static EquivalenceBreaker create(
            ContentResolver contentResolver,
            LookupEntryStore entryStore,
            LookupWriter directLookupWriter,
            LookupWriter explicitLookupWriter
    ) {
        return new EquivalenceBreaker(contentResolver, entryStore, directLookupWriter, explicitLookupWriter);
    }

    /**
     * @param source The item we want to remove equivalences from
     * @param sourceLE The lookup record of the source item
     * @param directEquivUrisToRemove The direct equivalence uris we want to remove.
     *                                If source uri is included, this method will ignore it.
     * @param explicitEquivUrisToRemove The explicit equivalence uris we want to remove.
     *                                If source uri is included, this method will ignore it.
     */
    public void removeFromSet(
            Described source,
            LookupEntry sourceLE,
            Set<String> directEquivUrisToRemove,
            Set<String> explicitEquivUrisToRemove
    ){
        removeDirectEquivs(source, sourceLE, directEquivUrisToRemove);
        removeExplicitEquivs(source, sourceLE, explicitEquivUrisToRemove);
    }

    /**
     * Use {@link #removeFromSet(Described, LookupEntry, Set, Set)} or {@link #removeEquivs}
     */
    @Deprecated
    public void removeFromSet(
            Described source,
            LookupEntry sourceLE,
            Set<String> directEquivUrisToRemove
    ){
        removeDirectEquivs(source, sourceLE, directEquivUrisToRemove);
    }

    /**
     * Keep in mind this is very inefficient, as if you are removing everything, you are calling
     * this multiple times, say e.g. you are unpublishing something and nuking the whole equiv set,
     * you will be re-resolving, recalculating and re-saving the transitive sets of
     * everything in the set all the time.
     * @param sourceUri The uri of the item, that we want to remove the equivalence from
     * @param directEquivUriToRemove The equivalence to remove from the source item
     *
     * NOTE: This will not remove explicit equivs. Furthermore, if there are any equivs in the
     *       content record of the sourceUri, then they will be added in the explicit equvs in
     *       the lookup record.
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

        if (!lookupEntry.getDirectEquivalents().getOutgoing()
                .stream()
                .map(LookupRef.TO_URI::apply)
                .collect(Collectors.toList())
                .contains(directEquivUriToRemove)) {
            throw new IllegalArgumentException("Direct equivalence to " 
                            + directEquivUriToRemove + " not found");
        }
        
        Iterable<LookupRef> filteredRefs = lookupEntry.getDirectEquivalents().getOutgoing()
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
        
        directLookupWriter.writeLookup(ContentRef.valueOf(source), equivalents, Publisher.all());
    }

    public void removeDirectEquivs(
            Described source,
            LookupEntry sourceLE,
            Set<String> directEquivUrisToRemove
    ) {
        removeEquivs(source, sourceLE, directEquivUrisToRemove, false);
    }

    public void removeExplicitEquivs(
            Described source,
            LookupEntry sourceLE,
            Set<String> explicitEquivUrisToRemove
    ) {
        removeEquivs(source, sourceLE, explicitEquivUrisToRemove, true);
    }

    private void removeEquivs(
            Described source,
            LookupEntry sourceLE,
            Set<String> equivUrisToRemove,
            boolean explicit
    ){

        if(equivUrisToRemove.isEmpty()){    // nothing to do here
            return;
        }
        Set<LookupRef> existingEquivs;
        LookupWriter writer;
        if (explicit) {
            existingEquivs = sourceLE.getExplicitEquivalents().getOutgoing();
            writer = explicitLookupWriter;
        } else {
            existingEquivs = sourceLE.getDirectEquivalents().getOutgoing();
            writer = directLookupWriter;
        }

        //make sure to not remove yourself.
        equivUrisToRemove =
                Sets.difference(equivUrisToRemove, ImmutableSet.of(source.getCanonicalUri()));

        Set<String> newEquivUris = calculateNewEquivUris(equivUrisToRemove, existingEquivs);
        List<ContentRef> newEquivs = urisToContentRef(newEquivUris);

        writer.writeLookup(ContentRef.valueOf(source), newEquivs, Publisher.all());
    }

    private Set<String> calculateNewEquivUris(Set<String> equivUrisToRemove, Set<LookupRef> existingEquivs) {
        Set<String> existingEquivUris = existingEquivs
                .stream()
                .map(LookupRef::uri)
                .collect(Collectors.toSet());
        return Sets.difference(existingEquivUris, equivUrisToRemove);
    }

    private List<ContentRef> urisToContentRef(Set<String> uris) {
        ResolvedContent resolvedEquivList = contentResolver.findByCanonicalUris(uris);
        if (resolvedEquivList != null) {
            return resolvedEquivList.getAllResolvedResults()
                    .stream()
                    .filter(Described.class::isInstance)
                    .map(Described.class::cast)
                    .map(ContentRef.FROM_CONTENT::apply)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}
