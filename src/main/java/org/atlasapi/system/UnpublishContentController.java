package org.atlasapi.system;


import java.util.NoSuchElementException;
import java.util.Optional;

import javax.servlet.http.HttpServletResponse;

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

    @RequestMapping(value = "/system/content/unpublish/{id}", method = RequestMethod.POST)
    public void unpublish(HttpServletResponse response, @PathVariable("id") String id) {

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
                                .findByUris(Lists.newArrayList(contentUri.uri()))
                                .getFirstValue()
                                .valueOrNull());

        // we throw lots of exceptions defensively
        if(! identified.isPresent()) {
            throw new NoSuchElementException(String.format(
                    "Unable to resolve Item from ID %d, URI %s", contentId, contentUri.uri()));
        }

        if(! (identified.get() instanceof Item)) {
            throw new RuntimeException((String.format(
                    "Identified %d is not Item", contentId)));
        }

        // now unpublish item (strictly we only need a Described but restricting to items is safer)
        Item item = (Item) identified.get();
        item.setActivelyPublished(false);
        contentWriter.createOrUpdate(item);

        response.setStatus(HttpServletResponse.SC_OK);
    }
}
