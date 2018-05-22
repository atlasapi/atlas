package org.atlasapi.equiv.update.www;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                15, 15, //The searcher cannot really cope with high load.
                60, TimeUnit.SECONDS,
                new CustomBlockingQueue<>(20000, (o1, o2) -> {
                    if(o1 == o2) {
                        return 0;
                    }
                    if(o1 instanceof Task && o2 instanceof Task) {
                        return ((Task) o1).compareTo((Task) o2);
                    }
                    if(o1 instanceof Task) {
                        return -1;
                    }
                    return 1;
                }
            )
        );
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        executor = threadPoolExecutor;
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
            @RequestParam(value = "ids", required = false, defaultValue = "") String ids,
            @RequestParam(value = "async", required = false, defaultValue = "false") boolean async
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

        List<Content> contents = resolved.getAllResolvedResults().stream()
                .filter(Content.class::isInstance)
                .map(Content.class::cast)
                .collect(MoreCollectors.toImmutableList());

        telescope.startReporting();

        if(!async && contents.size() == 1) {
            updateFor(contents.get(0), telescope, true).run();
        } else {
            Set<Future<?>> futures = contents.stream()
                    .map(c -> updateFor(c, telescope, !async))
                    .map(executor::submit)
                    .collect(MoreCollectors.toImmutableSet());
            if (!async) {
                try {
                    for(Future<?> future : futures) {
                        future.get();
                    }
                } catch (Exception e) {
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    telescope.endReporting();
                    return;
                }
            }
            log.info("Equivalence endpoint executor status: {}", executor);
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

    private Task updateFor(final Content content, OwlTelescopeReporter telescope, boolean priority) {
        return new Task(priority, () -> {
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
        });
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

    @RequestMapping(value = "/system/equivalence/status", method = RequestMethod.GET)
    public void getEquivalenceExecutorStatus(
            HttpServletResponse response
    ) throws IOException {
        response.getWriter().write(executor.toString());
        response.setStatus(OK.code());
    }

    private static final AtomicLong TASK_ID = new AtomicLong(0);

    private static class Task implements Runnable, Comparable<Task> {
        private final boolean priority;
        private final Runnable runnable;
        private final long taskId;

        public Task(boolean priority, Runnable runnable) {
            this.priority = priority;
            this.runnable = runnable;
            taskId = TASK_ID.incrementAndGet();
        }

        @Override
        public void run() {
            runnable.run();
        }

        @Override
        public int compareTo(Task t) {
            if(priority == t.priority) {
                return (int) (taskId - t.taskId);
            }
            if(priority) {
                return -1;
            }
            return 1;
        }
    }

    private static class CustomBlockingQueue<E> extends PriorityBlockingQueue<E> {
        //Capacity at which we stop adding async tasks
        private final int softMaxCapacity;

        public CustomBlockingQueue(int softMaxCapacity, Comparator<E> comparator) {
            super(softMaxCapacity, comparator); //underlying implementation is unbounded
            this.softMaxCapacity = softMaxCapacity;
        }

        @Override
        public boolean offer(E e) {
            //synchronous tasks can bypass the maximum capacity
            if(size() < softMaxCapacity || (e instanceof Task && ((Task) e).priority)) {
                return super.offer(e);
            }
            return false;
        }

    }

}
