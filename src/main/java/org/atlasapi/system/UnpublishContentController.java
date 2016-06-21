package org.atlasapi.system;


import java.util.NoSuchElementException;
import java.util.Optional;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.collect.Lists;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
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

    public UnpublishContentController(
            NumberToShortStringCodec idCodec,
            ContentResolver contentResolver,
            LookupEntryStore lookupEntryStore,
            ContentWriter contentWriter){

        this.idCodec = checkNotNull(idCodec);
        this.contentResolver = checkNotNull(contentResolver);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentWriter = checkNotNull(contentWriter);
    }

    @RequestMapping(value = "/system/content/publish/{id}", method = RequestMethod.POST)
    public void publish(HttpServletResponse response,
            @PathVariable("id") String id,
            @RequestParam(value = "publisher", required = false) String publisher) {

        setPublishStatusOfItem(id, Optional.ofNullable(publisher), true);
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @RequestMapping(value = "/system/content/unpublish/{id}", method = RequestMethod.POST)
    public void unpublish(HttpServletResponse response,
            @PathVariable("id") String id,
            @RequestParam(value = "publisher") String publisher) {

        setPublishStatusOfItem(id, Optional.ofNullable(publisher), false);
        response.setStatus(HttpServletResponse.SC_OK);
    }

    private void setPublishStatusOfItem(String id, Optional<String> publisher, boolean status) {
        // if we cannot resolve the ID we want a notfound exception
        Long contentId = idCodec.decode(id).longValue();
        LookupEntry contentUri = lookupEntryStore
                .entriesForIds(Lists.newArrayList(contentId))
                .iterator()
                .next();

        // let's get the content item now
        Optional<Identified> identified =
                Optional.ofNullable(
                        contentResolver
                                .findByCanonicalUris(Lists.newArrayList(contentUri.uri()))
                                .getFirstValue()
                                .valueOrNull());

        // we throw lots of exceptions defensively
        if(! identified.isPresent()) {
            throw new NoSuchElementException(String.format(
                    "Unable to resolve Item from ID %d, URI %s", contentId, contentUri.uri()));
        }

        // check publisher constraint is met
        Described described = (Described) identified.get();
        publisher.ifPresent(key -> {
            if (!key.equals(described.getPublisher().key())) {
                throw new RuntimeException((String.format(
                        "Described %d is not published by '%s'", contentId, publisher)));
            }
        });

        // change publish status
        described.setActivelyPublished(status);

        // now write in the appropriate manner
        if(described instanceof Item) {
            contentWriter.createOrUpdate((Item) described);
            return;
        }

        if(described instanceof Container) {
            contentWriter.createOrUpdate((Container) described);
            return;
        }

        throw new IllegalStateException((String.format(
                "Described %d is not Item/Container", contentId)));
    }
}
