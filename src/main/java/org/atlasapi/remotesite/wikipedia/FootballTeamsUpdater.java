package org.atlasapi.remotesite.wikipedia;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.*;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.organisation.OrganisationWriter;
import org.atlasapi.remotesite.wikipedia.football.FootballTeamsExtractor;
import org.atlasapi.remotesite.wikipedia.football.TeamsNamesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.scheduling.ScheduledTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

public class FootballTeamsUpdater extends ScheduledTask{
    private static Logger log = LoggerFactory.getLogger(FootballTeamsUpdater.class);

    private int simultaneousness;
    private int threadsToStart;

    private ListeningExecutorService executor;
    private final CountDownLatch countdown;

    private FetchMeister fetchMeister;
    private TeamsNamesSource titleSource;
    private FetchMeister.PreloadedArticlesQueue articleQueue;

    private FootballTeamsExtractor extractor;
    private OrganisationWriter writer;

    private UpdateProgress progress;
    private int totalTitles;

    public FootballTeamsUpdater(TeamsNamesSource titleSource, FetchMeister fetcher, FootballTeamsExtractor extractor, OrganisationWriter writer, int simultaneousness, int threadsToStart) {
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
        Iterable<String> titles = titleSource.getAllTeamNames();
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

        reportStatus(String.format("Processed: %d football teams (%d failed)", progress.getTotalProgress(), progress.getFailures()));
    }

    private void reduceProgress(UpdateProgress occurrence) {
        synchronized (this) {
            progress = progress.reduce(occurrence);
        }
        reportStatus(String.format("Processing: %d/%d football teams so far (%d failed)", progress.getTotalProgress(), totalTitles, progress.getFailures()));
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
                log.warn("Failed to process a football team", t);
                reduceProgress(UpdateProgress.FAILURE);
                processNext();
            }
        });
    }

    private ListenableFuture<Void> updateFootballTeam(ListenableFuture<Article> article) {
        return Futures.transform(article, new Function<Article, Void>() {
            public Void apply(Article article) {
                log.info("Processing football team article \"" + article.getTitle() + "\"");
                Organisation team = extractor.extract(article);
                writer.createOrUpdateOrganisation(team);
                return null;
            }
        }, executor);
    }
}
