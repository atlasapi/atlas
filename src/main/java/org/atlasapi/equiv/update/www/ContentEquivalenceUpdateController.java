package org.atlasapi.equiv.update.www;

import static com.metabroadcast.common.http.HttpStatusCode.NOT_FOUND;
import static com.metabroadcast.common.http.HttpStatusCode.OK;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Function;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.RootEquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

@Controller
public class ContentEquivalenceUpdateController {
    
    private static final Logger log = LoggerFactory.getLogger(ContentEquivalenceUpdateController.class);
    
    private final Splitter commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    private final EquivalenceUpdater<Content> contentUpdater;
    private final ContentResolver contentResolver;
    private final ExecutorService executor;
    private final SubstitutionTableNumberCodec codec;
    private final LookupEntryStore lookupEntryStore;

    public ContentEquivalenceUpdateController(EquivalenceUpdater<Content> contentUpdater,
            ContentResolver contentResolver, LookupEntryStore lookupEntryStore) {
        this.contentUpdater = new RootEquivalenceUpdater(contentResolver, contentUpdater);
        this.contentResolver = contentResolver;
        this.executor = Executors.newFixedThreadPool(5);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.lookupEntryStore = lookupEntryStore;
    }

    @RequestMapping(value = "/system/equivalence/update", method = RequestMethod.POST)
    public void runUpdate(HttpServletResponse response,
            @RequestParam(value="uris", required=false) String uris,
            @RequestParam(value="ids", required=false) String ids) throws IOException {

        if (Strings.isNullOrEmpty(uris) && Strings.isNullOrEmpty(ids)) {
            throw new IllegalArgumentException("Must specify at least one of 'uris' or 'ids'");
        }

        Iterable<String> allRequestedUris = Iterables.concat(commaSplitter.split(uris), urisFor(ids));
        ResolvedContent resolved = contentResolver.findByCanonicalUris(allRequestedUris);

        if (resolved.isEmpty()) {
            response.setStatus(NOT_FOUND.code());
            response.setContentLength(0);
            return;
        }

        for (Content content : Iterables.filter(resolved.getAllResolvedResults(), Content.class)) {
            executor.submit(updateFor(content));
        }
        response.setStatus(OK.code());

    }

    private Iterable<String> urisFor(String csvIds) {
        if (Strings.isNullOrEmpty((csvIds))) {
            return ImmutableSet.of();
        }
        Iterable<Long> ids = Iterables.transform(commaSplitter.split(csvIds), new Function<String, Long>() {

            @Override public Long apply(String input) {
                return codec.decode(input).longValue();
            }
        });
        return Iterables.transform(lookupEntryStore.entriesForIds(ids), LookupEntry.TO_ID);
    }

    private Runnable updateFor(final Content content) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    contentUpdater.updateEquivalences(content);
                    log.info("Finished updating {}",content);
                } catch (Exception e) {
                    log.error(content.toString(), e);
                }
            }
        };
    }

}
