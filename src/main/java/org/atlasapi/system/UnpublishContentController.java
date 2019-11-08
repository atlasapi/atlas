package org.atlasapi.system;

import java.util.NoSuchElementException;
import java.util.Optional;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;


@Controller
public class UnpublishContentController {

    private final ContentResolver contentResolver;
    private final LookupEntryStore lookupEntryStore;
    private final NumberToShortStringCodec idCodec;
    private final ContentWriter contentWriter;
    private final ContentWriter noEquivalenceContentWriter;
    private final EquivalenceBreaker equivalenceBreaker;

    public UnpublishContentController(
            NumberToShortStringCodec idCodec,
            ContentResolver contentResolver,
            LookupEntryStore lookupEntryStore,
            ContentWriter contentWriter,
            ContentWriter noEquivalenceContentWriter,
            EquivalenceBreaker equivalenceBreaker
    ) {
        this.idCodec = checkNotNull(idCodec);
        this.contentResolver = checkNotNull(contentResolver);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentWriter = checkNotNull(contentWriter);
        this.noEquivalenceContentWriter = checkNotNull(noEquivalenceContentWriter);
        this.equivalenceBreaker = checkNotNull(equivalenceBreaker);
    }

    @RequestMapping(value = "/system/content/publish", method = RequestMethod.POST)
    public void publish(HttpServletResponse response,
            @RequestParam(value = "id", required = false) String id,
            @RequestParam(value = "uri", required = false) String uri,
            @RequestParam(value = "publisher", required = false) String publisher
    ) {
        setPublishStatusOfItem(
                Optional.ofNullable(id),
                Optional.ofNullable(uri),
                Optional.ofNullable(publisher),
                true
        );
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @RequestMapping(value = "/system/content/unpublish", method = RequestMethod.POST)
    public void unpublish(HttpServletResponse response,
            @RequestParam(value = "id", required = false) String id,
            @RequestParam(value = "uri", required = false) String uri,
            @RequestParam(value = "publisher") String publisher
    ) {
        setPublishStatusOfItem(
                Optional.ofNullable(id),
                Optional.ofNullable(uri),
                Optional.ofNullable(publisher),
                false
        );
        response.setStatus(HttpServletResponse.SC_OK);
    }

    private void setPublishStatusOfItem(
            Optional<String> id,
            Optional<String> uri,
            Optional<String> publisher,
            boolean status
    ) {
        // if we cannot resolve the ID we want a notfound exception
        LookupEntry lookupEntry = getLookupEntry(id, uri);

        Optional<Identified> identified = resolveContent(lookupEntry);
        Described described = validatePublisher(publisher, identified);

        described.setActivelyPublished(status);
        if(!status){
            removeEquivSetOfItem(described, lookupEntry);
            writeUpdate(described, noEquivalenceContentWriter); //write to db ignoring equivs
        }
        else {
            writeUpdate(described, contentWriter);
        }
    }

    private LookupEntry getLookupEntry(Optional<String> id, Optional<String> uri) {
        LookupEntry lookupEntry;
        if(id.isPresent()){
            Long contentId = idCodec.decode(id.get()).longValue();
            lookupEntry = lookupEntryStore
                    .entriesForIds(Lists.newArrayList(contentId))
                    .iterator()
                    .next();
        } else if (uri.isPresent()){
            lookupEntry = lookupEntryStore
                    .entriesForCanonicalUris(Lists.newArrayList(uri.get()))
                    .iterator()
                    .next();
        } else {
            throw new IllegalArgumentException("id / uri parameter not specified");
        }
        return lookupEntry;
    }

    private Optional<Identified> resolveContent(LookupEntry lookupEntry) {
        Optional<Identified> identified = Optional.ofNullable(
                contentResolver
                        .findByCanonicalUris(Lists.newArrayList(lookupEntry.uri()))
                        .getFirstValue()
                        .valueOrNull()
        );

        // we throw lots of exceptions defensively
        if(! identified.isPresent()) {
            throw new NoSuchElementException(String.format(
                    "Unable to resolve Item from ID %d, URI %s", lookupEntry.id(), lookupEntry.uri()));
        }
        return identified;
    }

    private Described validatePublisher(
            Optional<String> publisher,
            Optional<Identified> identified
    ) {
        Described described = (Described) identified.get();
        publisher.ifPresent(key -> {
            if (!key.equals(described.getPublisher().key())) {
                throw new RuntimeException((String.format(
                        "Described %d is not published by '%s'", described.getId(), publisher)));
            }
        });
        return described;
    }

    /**
     * This will take an item, and remove all direct equivalences from it
     */
    private void removeEquivSetOfItem(Described described, LookupEntry lookupEntry){

        ImmutableSet<String> allDirectEquivs = lookupEntry.directEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<String> allExplicitEquivs = lookupEntry.explicitEquivalents()
                .stream()
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());

        equivalenceBreaker.removeFromSet(described, lookupEntry, allDirectEquivs, allExplicitEquivs);
    }

    private void writeUpdate(Described described, ContentWriter contentWriter) {
        if(described instanceof Item) {
            contentWriter.createOrUpdate((Item) described);
            return;
        }

        if(described instanceof Container) {
            contentWriter.createOrUpdate((Container) described);
            return;
        }

        throw new IllegalStateException((String.format(
                "Described %d is not Item/Container", described.getId())));
    }
}