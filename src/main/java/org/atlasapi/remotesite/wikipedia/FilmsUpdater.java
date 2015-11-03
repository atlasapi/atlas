package org.atlasapi.remotesite.wikipedia;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.atlasapi.media.entity.Film;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.wikipedia.FetchMeister.PreloadedArticlesQueue;
import org.atlasapi.remotesite.wikipedia.film.FilmArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * A task which iterates over all films from a {@link FilmArticleTitleSource}, adding each of them in turn.
 */
public class FilmsUpdater extends ScheduledTask {
    private static Logger log = LoggerFactory.getLogger(FilmsUpdater.class);

    /** How many films we try to process at once. */
    private int simultaneousness;
    /** How many threads we use for all the deferred processing. */
    private int threadsToStart;

    private ListeningExecutorService executor;
    private final CountDownLatch countdown;

    private FetchMeister fetchMeister;
    private FilmArticleTitleSource titleSource;
    private PreloadedArticlesQueue articleQueue;
    
    private FilmExtractor extractor;
    private ContentWriter writer;
    
    private UpdateProgress progress;
    private int totalTitles;
    
    public FilmsUpdater(FilmArticleTitleSource titleSource, FetchMeister fetcher, FilmExtractor extractor, ContentWriter writer,  int simultaneousness, int threadsToStart) {
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
        Iterable<String> titles = titleSource.getAllFilmArticleTitles();
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

        reportStatus(String.format("Processed: %d films (%d failed)", progress.getTotalProgress(), progress.getFailures()));
    }
    
    private void reduceProgress(UpdateProgress occurrence) {
        synchronized (this) {
            progress = progress.reduce(occurrence);
        }
        reportStatus(String.format("Processing: %d/%d films so far (%d failed)", progress.getTotalProgress(), totalTitles, progress.getFailures()));
    }

    private void processNext() {
        Optional<ListenableFuture<Article>> next = articleQueue.fetchNextBaseArticle();
        if(!shouldContinue() || !next.isPresent()) {
            countdown.countDown();
            return;
        }
        Futures.addCallback(updateFilm(next.get()), new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                reduceProgress(UpdateProgress.SUCCESS);
                processNext();
            }
            @Override
            public void onFailure(Throwable t) {
                log.warn("Failed to process a film", t);
                reduceProgress(UpdateProgress.FAILURE);
                processNext();
            }
        });
    }
    
    private ListenableFuture<Void> updateFilm(ListenableFuture<Article> article) {
        return Futures.transform(article, new Function<Article, Void>() {
            public Void apply(Article article) {
                log.info("Processing film article \"" + article.getTitle() + "\"");
                Film flim = extractor.extract(article);
                writer.createOrUpdate(flim);
                return null;
            }
        }, executor);
    }
}
