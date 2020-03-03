package org.atlasapi.remotesite.pa.deletes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class un-publishes old PA items. Currently it only supports items and not containers as it
 * does not have the logic to un-publish a container's children.
 */
public class ExistingItemUnPublisher {

    /**
     * Disabled until the long term solution for cleaning-up duplicates is introduced.
     */
    private final boolean disabled;

    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final LookupEntryStore lookupEntryStore;
    private final EquivalenceBreaker equivalenceBreaker;

    private ExistingItemUnPublisher(
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            LookupEntryStore lookupEntryStore,
            EquivalenceBreaker equivalenceBreaker,
            boolean disabled
    ) {
        this.disabled = disabled;

        this.contentResolver = checkNotNull(contentResolver);
        this.contentWriter = checkNotNull(contentWriter);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.equivalenceBreaker = checkNotNull(equivalenceBreaker);
    }

    public static ExistingItemUnPublisher create(
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            LookupEntryStore lookupEntryStore,
            EquivalenceBreaker equivalenceBreaker,
            boolean disabled
    ) {
        return new ExistingItemUnPublisher(
                contentResolver,
                contentWriter,
                lookupEntryStore,
                equivalenceBreaker,
                disabled
        );
    }

    public void unPublishItems(String uri) {
        if (disabled) {
            return;
        }

        unPublishItemsInternal(uri);
    }

    private void unPublishItemsInternal(String uri) {
        Iterable<LookupEntry> lookupEntries = lookupEntryStore.entriesForCanonicalUris(
                ImmutableList.of(uri)
        );

        ImmutableMap<String, Item> items = StreamSupport.stream(
                lookupEntries.spliterator(),
                false
        )
                .map(entry -> contentResolver.findByCanonicalUris(
                        ImmutableList.of(entry.uri())
                ))
                .map(resolvedContent -> resolvedContent.getFirstValue().toOptional())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(content -> content instanceof Item)
                .map(content -> (Item) content)
                .filter(Described::isActivelyPublished)
                .collect(MoreCollectors.toImmutableMap(
                        Identified::getCanonicalUri,
                        item -> item
                ));

        items.values().forEach(this::unPublishItems);

        StreamSupport.stream(
                lookupEntries.spliterator(),
                false
        )
                .filter(entry -> items.containsKey(entry.uri()))
                .forEach(this::removeEntryFromEquivSet);
    }

    private void unPublishItems(Item item) {
        item.setActivelyPublished(false);
        contentWriter.createOrUpdate(item);
    }

    private void removeEntryFromEquivSet(LookupEntry lookupEntry){
        String lookupEntryUri = lookupEntry.uri();
        lookupEntry.getDirectEquivalents().getOutgoing()
                .stream()
                .map(LookupRef::uri)
                .filter(lookupRefUri -> !lookupRefUri.equals(lookupEntryUri))
                .forEach(lookupRefUri -> equivalenceBreaker.removeFromSet(
                        lookupEntryUri,
                        lookupRefUri
                ));
    }
}
