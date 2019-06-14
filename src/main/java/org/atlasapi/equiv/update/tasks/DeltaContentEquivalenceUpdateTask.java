package org.atlasapi.equiv.update.tasks;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.mongo.LastUpdatedContentFinder;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;
import org.atlasapi.util.AlwaysBlockingQueue;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

/**
 * This task will redo equivalence for all content updated since the given date, rather than using
 * the Content Lister's progress. It runs with 15 threads, and the first person who needs to use
 * it elsewhere gets convert the executor to a parameter.
 */
public final class DeltaContentEquivalenceUpdateTask extends ScheduledTask {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final EquivalenceUpdater<Content> updater;
    private final Set<String> ignored;
    private final OwlTelescopeReporter telescope;
    private final ThreadPoolExecutor executor;

    private String schedulingKey = "delta-equivalence";
    private Publisher publisher;
    private volatile UpdateProgress progress = UpdateProgress.START;
    private Period delta;
    private LastUpdatedContentFinder contentFinder;

    public DeltaContentEquivalenceUpdateTask(LastUpdatedContentFinder contentFinder,
            EquivalenceUpdater<Content> updater,
            Set<String> ignored) {
        this.contentFinder = contentFinder;
        this.updater = updater;
        this.ignored = ignored;
        telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.EQUIVALENCE,
                Event.Type.EQUIVALENCE
        );

        executor = new ThreadPoolExecutor(
                15, 15, //The searcher cannot really cope with high load.
                60, TimeUnit.SECONDS,
                new AlwaysBlockingQueue<>(300)
        );
    }

    public DeltaContentEquivalenceUpdateTask forPublisher(Publisher publisher) {
        this.publisher = publisher;
        this.schedulingKey = publisher.key() + "-delta-equivalence";
        return this;
    }

    public DeltaContentEquivalenceUpdateTask forLast(Period delta) {
        this.delta = delta;
        return this;
    }

    @Override
    protected void runTask() {

        DateTime since = DateTime.now().minus(delta);
        onStart(since);

        Iterator<Content> contents = contentFinder.updatedSince(publisher, since);
        try {
            while(shouldContinue() && contents.hasNext()) {
                executor.submit(handle(contents.next()));
            }
        } catch (Exception e) {
            log.error(getName(), e);
        }
        finally {
            onFinish();
        }
    }

    protected void onStart(DateTime date) {
        telescope.startReporting();
        log.info("Started {} job from {}", schedulingKey, date.toString());
        this.progress = UpdateProgress.START;
    }

    protected Runnable handle(Content content) {
       return () -> {
           if (!ignored.contains(content.getCanonicalUri())) {
               reportStatus(String.format("%s. Processing %s.", progress, content));
               try {
                   updater.updateEquivalences(content, telescope);
                   progress = progress.reduce(SUCCESS);
               } catch (Exception e) {
                   log.error(content.toString(), e);
                   progress = progress.reduce(FAILURE);
               }
           }
       };

    }

    protected void onFinish() {
        log.info("Finished {} job.", schedulingKey);
        telescope.endReporting();
    }

}
