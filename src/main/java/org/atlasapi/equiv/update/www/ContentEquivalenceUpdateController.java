package org.atlasapi.equiv.update.www;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.RootEquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.metabroadcast.common.http.HttpStatusCode.NOT_FOUND;
import static com.metabroadcast.common.http.HttpStatusCode.OK;

@Controller
public class ContentEquivalenceUpdateController {

    private static final Logger log = LoggerFactory.getLogger(
            ContentEquivalenceUpdateController.class
    );

    private final Splitter commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    private final EquivalenceUpdater<Content> contentUpdater;
    private final ContentResolver contentResolver;
    private final ExecutorService executor;
    private final SubstitutionTableNumberCodec codec;
    private final LookupEntryStore lookupEntryStore;
    private final ObjectMapper mapper;

    private ContentEquivalenceUpdateController(
            EquivalenceUpdater<Content> contentUpdater,
            ContentResolver contentResolver,
            LookupEntryStore lookupEntryStore
    ) {
        this.contentUpdater = RootEquivalenceUpdater.create(contentResolver, contentUpdater);
        this.contentResolver = contentResolver;
        this.executor = Executors.newFixedThreadPool(5);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.lookupEntryStore = lookupEntryStore;
        this.mapper = new ObjectMapper();
    }

    public static ContentEquivalenceUpdateController create(
            EquivalenceUpdater<Content> contentUpdater,
            ContentResolver contentResolver,
            LookupEntryStore lookupEntryStore
    ) {
        return new ContentEquivalenceUpdateController(
                contentUpdater,
                contentResolver,
                lookupEntryStore
        );
    }

    @RequestMapping(value = "/system/equivalence/update", method = RequestMethod.POST)
    public void runUpdate(
            HttpServletResponse response,
            @RequestParam(value = "uris", required = false, defaultValue = "") String uris,
            @RequestParam(value = "ids", required = false, defaultValue = "") String ids
    ) throws IOException {

        if (Strings.isNullOrEmpty(uris) && Strings.isNullOrEmpty(ids)) {
            throw new IllegalArgumentException("Must specify at least one of 'uris' or 'ids'");
        }

        Iterable<String> allRequestedUris = Iterables.concat(
                commaSplitter.split(uris),
                urisFor(ids)
        );
        ResolvedContent resolved = contentResolver.findByCanonicalUris(allRequestedUris);

        if (resolved.isEmpty()) {
            response.setStatus(NOT_FOUND.code());
            response.setContentLength(0);
            return;
        }

        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.MANUAL_EQUIVALENCE,
                Event.Type.EQUIVALENCE
        );

        telescope.startReporting();

        for (Content content : Iterables.filter(resolved.getAllResolvedResults(), Content.class)) {
            executor.submit(updateFor(content, telescope));
        }

        telescope.endReporting();

        response.setStatus(OK.code());

    }

    private Iterable<String> urisFor(String csvIds) {
        if (Strings.isNullOrEmpty((csvIds))) {
            return ImmutableSet.of();
        }
        Iterable<Long> ids = StreamSupport.stream(commaSplitter.split(csvIds).spliterator(), false)
                .map(input -> codec.decode(input).longValue())
                .collect(Collectors.toList());

        return StreamSupport.stream(lookupEntryStore.entriesForIds(ids).spliterator(), false)
                .map(LookupEntry::uri)
                .collect(Collectors.toList());
    }

    private Runnable updateFor(final Content content, OwlTelescopeReporter telescope) {
        return () -> {
            try {
                contentUpdater.updateEquivalences(content, telescope);
                log.info("Finished updating {}", content);
            } catch (Exception e) {
                log.error(content.toString(), e);
                telescope.reportFailedEvent(
                        e.toString(),
                        content
                );
            }
        };
    }

    @RequestMapping(value = "/system/equivalence/configuration", method = RequestMethod.GET)
    public void getEquivalenceConfiguration(
            HttpServletResponse response,
            @Nullable @RequestParam(value = "sources", required = false) List<String> sources
    ) throws IOException {
        ImmutableSet<Publisher> requestedSources;

        if (sources != null) {
            requestedSources = sources.stream()
                    .map(Publisher::fromKey)
                    .map(Maybe::requireValue)
                    .collect(MoreCollectors.toImmutableSet());
        } else {
            requestedSources = Publisher.all();
        }

        EquivalenceUpdaterMetadata metadata = contentUpdater.getMetadata(requestedSources);

        mapper.writeValue(
                response.getWriter(),
                metadata
        );
        response.setStatus(OK.code());
    }

}
