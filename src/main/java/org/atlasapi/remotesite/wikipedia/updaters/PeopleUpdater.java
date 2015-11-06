package org.atlasapi.remotesite.wikipedia.updaters;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.people.PersonWriter;
import org.atlasapi.remotesite.wikipedia.people.PeopleExtractor;
import org.atlasapi.remotesite.wikipedia.people.PeopleNamesSource;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.FetchMeister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

public class PeopleUpdater extends ScheduledTask {
    private static Logger log = LoggerFactory.getLogger(FootballTeamsUpdater.class);

    private int simultaneousness;
    private int threadsToStart;

    private ListeningExecutorService executor;
    private final CountDownLatch countdown;

    private FetchMeister fetchMeister;
    private PeopleNamesSource titleSource;
    private FetchMeister.PreloadedArticlesQueue articleQueue;

    private PeopleExtractor extractor;
    private PersonWriter writer;

    private UpdateProgress progress;
    private int totalTitles;

    public PeopleUpdater(PeopleNamesSource titleSource, FetchMeister fetcher, PeopleExtractor extractor, PersonWriter writer, int simultaneousness, int threadsToStart) {
        this.titleSource = checkNotNull(titleSource);
        this.fetchMeister = checkNotNull(fetcher);
        this.extractor = checkNotNull(extractor);
        this.writer = checkNotNull(writer);
        this.simultaneousness = simultaneousness;
        this.threadsToStart = threadsToStart;
        this.countdown = new CountDownLatch(simultaneousness);
    }

    @Override
    protected void runTask() {
        reportStatus("Starting...");
        progress = UpdateProgress.START;
        fetchMeister.start();
        Iterable<String> titles = titleSource.getAllPeopleNames();
        articleQueue = fetchMeister.queueForPreloading(titles);
        totalTitles = Iterables.size(titles);
        executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadsToStart));
        for(int i=0; i<simultaneousness; ++i) {
            processNext();
        }
        while(true) {
            try {
                countdown.await();
                break;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        executor.shutdown();
        fetchMeister.cancelPreloading(articleQueue);
        articleQueue = null;
        fetchMeister.stop();

        reportStatus(String.format("Processed: %d people (%d failed)", progress.getTotalProgress(), progress.getFailures()));
    }

    private void reduceProgress(UpdateProgress occurrence) {
        synchronized (this) {
            progress = progress.reduce(occurrence);
        }
        reportStatus(String.format("Processing: %d/%d people so far (%d failed)", progress.getTotalProgress(), totalTitles, progress.getFailures()));
    }

    private void processNext() {
        Optional<ListenableFuture<Article>> next = articleQueue.fetchNextBaseArticle();
        if(!shouldContinue() || !next.isPresent()) {
            countdown.countDown();
            return;
        }
        Futures.addCallback(updateFootballTeam(next.get()), new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                reduceProgress(UpdateProgress.SUCCESS);
                processNext();
            }
            @Override
            public void onFailure(Throwable t) {
                log.warn("Failed to process a ", t);
                reduceProgress(UpdateProgress.FAILURE);
                processNext();
            }
        });
    }

    private ListenableFuture<Void> updateFootballTeam(ListenableFuture<Article> article) {
        return Futures.transform(article, new Function<Article, Void>() {
            public Void apply(Article article) {
                log.info("Processing person's article \"" + article.getTitle() + "\"");
                Person person = extractor.extract(article);
                writer.createOrUpdatePerson(person);
                return null;
            }
        }, executor);
    }
}
