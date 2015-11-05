package org.atlasapi.remotesite.wikipedia.updaters;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Callback;
import org.atlasapi.remotesite.wikipedia.wikiparsers.FetchMeister;
import org.atlasapi.remotesite.wikipedia.wikiparsers.FetchMeister.PreloadedArticlesQueue;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.atlasapi.remotesite.wikipedia.television.BrandInfoboxScraper;
import org.atlasapi.remotesite.wikipedia.television.EpisodeListScraper;
import org.atlasapi.remotesite.wikipedia.television.ScrapedBrandInfobox;
import org.atlasapi.remotesite.wikipedia.television.ScrapedEpisode;
import org.atlasapi.remotesite.wikipedia.television.ScrapedFlatHierarchy;
import org.atlasapi.remotesite.wikipedia.television.SeasonSectionScraper;
import org.atlasapi.remotesite.wikipedia.television.TvBrandArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.television.TvBrandArticleTitleSource.TvIndexingException;
import org.atlasapi.remotesite.wikipedia.television.TvBrandHierarchy;
import org.atlasapi.remotesite.wikipedia.television.TvBrandHierarchyExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sweble.wikitext.lazy.preprocessor.LazyPreprocessedPage;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

public final class TvBrandHierarchyUpdater extends ScheduledTask {
    private static final Logger log = LoggerFactory.getLogger(TvBrandHierarchyUpdater.class);
    
    /** How many brands we try to process at once. */
    private int simultaneousness;
    /** How many threads we use for all the deferred processing. */
    private int threadsToStart;
    
    private ListeningExecutorService executor;
    private final CountDownLatch countdown;
    
    private final FetchMeister fetchMeister;
    private final TvBrandArticleTitleSource titleSource;
    private PreloadedArticlesQueue articleQueue;
    
    private final TvBrandHierarchyExtractor extractor;
    private final ContentWriter writer;
    
    private UpdateProgress progress;
    private int totalTitles;
    
    public TvBrandHierarchyUpdater(TvBrandArticleTitleSource titleSource, FetchMeister fetchMeister, TvBrandHierarchyExtractor extractor, ContentWriter writer, int simultaneousness, int threadsToStart) {
        this.fetchMeister = Preconditions.checkNotNull(fetchMeister);
        this.titleSource = Preconditions.checkNotNull(titleSource);
        this.extractor = Preconditions.checkNotNull(extractor);
        this.writer = Preconditions.checkNotNull(writer);
        this.simultaneousness = simultaneousness;
        this.threadsToStart = threadsToStart;
        this.countdown = new CountDownLatch(simultaneousness);
    }
    
    private Iterable<String> fetchTitles(int attempts) throws TvIndexingException {
        TvIndexingException failure = null;
        for(int i=0; i < attempts; ++i) {
            try {
                return titleSource.getAllTvBrandArticleTitles();
            } catch (TvIndexingException e) {
                log.warn("Failed once to fetch TV article titles; " + (attempts-i) + " attempts left,", e);
                failure = e;
            }
        }
        throw failure;
    }
    
    /**
     * Goes and iterates over all the given titles.
     */
    @Override
    public void runTask() {
        reportStatus("Starting...");
        progress = UpdateProgress.START;
        fetchMeister.start();
        Iterable<String> titles;
        try {
            titles = fetchTitles(3);
        } catch (TvIndexingException e) {
            log.error("Failed to fetch TV article titles, aborting :(");
            return;
        }
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
        reportStatus(String.format("Processed: %d brands (%d failed)", progress.getTotalProgress(), progress.getFailures()));
    }
    
    private void reduceProgress(UpdateProgress occurrence) {
        synchronized (this) {
            progress = progress.reduce(occurrence);
        }
        reportStatus(String.format("Processing: %d/%d brands so far (%d failed)", progress.getTotalProgress(), totalTitles, progress.getFailures()));
    }

    private void processNext() {
        Optional<ListenableFuture<Article>> next = articleQueue.fetchNextBaseArticle();
        if(!shouldContinue() || !next.isPresent()) {
            countdown.countDown();
            return;
        }
        Futures.addCallback(updateBrand(next.get()), new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                reduceProgress(UpdateProgress.SUCCESS);
                processNext();
            }
            @Override
            public void onFailure(Throwable t) {
                log.warn("Failed to process a TV brand", t);
                reduceProgress(UpdateProgress.FAILURE);
                processNext();
            }
        });
    }
    
    /**
     * A useful function to feed to {@link Futures#transform} -- converts a bunch of immutable lists of {@link EpisodeListScraper#Result} into one long list, by concatting them.
     */
    private static final Function<List<ImmutableCollection<ScrapedEpisode>>, ImmutableCollection<ScrapedEpisode>> CONCAT = new Function<List<ImmutableCollection<ScrapedEpisode>>, ImmutableCollection<ScrapedEpisode>>() {
        public ImmutableCollection<ScrapedEpisode> apply(List<ImmutableCollection<ScrapedEpisode>> input) {
            ImmutableList.Builder<ScrapedEpisode> b = ImmutableList.builder();
            for(ImmutableCollection<ScrapedEpisode> c : input) {
                if(c != null) {
                    b.addAll(c);
                }
            }
            return b.build();
        }
    };
    
    /**
     * Scrapes out all the episodes listed in {@code epList}, making sure each knows (where possible and appropriate) the season from which it came.
     */
    private ListenableFuture<ImmutableCollection<ScrapedEpisode>> extractSeasons(ListenableFuture<Article> epList) {
        return Futures.transform(epList, new AsyncFunction<Article, ImmutableCollection<ScrapedEpisode>>() {
                public ListenableFuture<ImmutableCollection<ScrapedEpisode>> apply(final Article epList) {
                    final ImmutableList.Builder<ListenableFuture<ImmutableCollection<ScrapedEpisode>>> builder = ImmutableList.builder();
                    log.debug("Commencing seasonSectionScraping on \""+ epList.getTitle() +"\"");
                    
                    final Boolean[] foundASection = {false};
                    new SeasonSectionScraper(new Callback<SeasonSectionScraper.Result>() {
                        public void have(final SeasonSectionScraper.Result season) {
                            foundASection[0] = true;
                            log.debug("Extracting on "+epList.getTitle()+" "+season.name);
                            builder.add(extractFlatEpisodes(season.content, season, false, season.name));
                        }
                    }).go(SwebleHelper.parse(epList.getMediaWikiSource()));
                    
                    if(foundASection[0]){  // then this is a page with sections and everything worked properly
                        return Futures.transform(Futures.successfulAsList(builder.build()), CONCAT);
                    } else {  // otherwise, there were no sections – we should just scan for episodes, leaving out the concept of seasons
                        return extractFlatEpisodes(epList.getMediaWikiSource(), null, false, epList.getTitle());
                    }
                }
            }, executor);
    }

    /**
     * Scrapes out episode records by preprocessing the given text.
     * @param source The text to parse.
     * @param season A season result to inject into the episodes.
     * @param includeOnly Is this being called on an article that was included as a template? If so, we choose to only parse the {@code includeOnly} section of the source.
     * @param context For logging/debug purposes, some explanation of the text on which this is being called.
     */
    private ListenableFuture<ImmutableCollection<ScrapedEpisode>> extractFlatEpisodes(String source, final SeasonSectionScraper.Result season, boolean includeOnly, final String context) {
        final ImmutableList.Builder<ListenableFuture<ImmutableCollection<ScrapedEpisode>>> laters = ImmutableList.builder();
        final ImmutableList.Builder<ScrapedEpisode> immediates = ImmutableList.builder();

        LazyPreprocessedPage section = SwebleHelper.preprocess(source, includeOnly);
        new EpisodeListScraper(new Callback<ScrapedEpisode>() {
            public void have(ScrapedEpisode result) {
                immediates.add(result);
            }
        }, new Callback<String>() {
            public void have(String includeTitle) {
                log.debug("Queueing fetch of \""+ includeTitle +"\" as child list for " + context);
                laters.add(Futures.transform(fetchMeister.fetchChildArticle(includeTitle), new AsyncFunction<Article, ImmutableCollection<ScrapedEpisode>>() {
                    public ListenableFuture<ImmutableCollection<ScrapedEpisode>> apply(Article childArticle) {
                        return extractFlatEpisodes(childArticle.getMediaWikiSource(), season, true, childArticle.getTitle());
                    }
                }, executor));
            }
        }).withSeason(season).go(section);
        ImmutableCollection<ScrapedEpisode> immeds = immediates.build();
        laters.add(Futures.immediateFuture(immeds));
        return Futures.transform(Futures.successfulAsList(laters.build()), CONCAT);
    }
    
    /**
     * Does all subfetching/resolution/writing for a given TV brand article.
     */
    private ListenableFuture<Void> updateBrand(final ListenableFuture<Article> baseArticle) {

        ListenableFuture<ScrapedBrandInfobox> brandInfobox = Futures.transform(baseArticle, new Function<Article,ScrapedBrandInfobox>() {
            public ScrapedBrandInfobox apply(final Article baseArticle) {
                LazyPreprocessedPage preprocess = SwebleHelper.preprocess(baseArticle.getMediaWikiSource(), true);
                ScrapedBrandInfobox res;
                final LinkedList<ScrapedBrandInfobox> it = Lists.newLinkedList();
                new BrandInfoboxScraper(new Callback<ScrapedBrandInfobox>() {
                    public void have(ScrapedBrandInfobox thing) {
                        it.add(thing);
                    }
                }).go(preprocess);
                return it.getFirst();
            }
        }, executor);

        ListenableFuture<ImmutableCollection<ScrapedEpisode>> externalListedEpisodes;
        externalListedEpisodes = Futures.transform(brandInfobox, new AsyncFunction<ScrapedBrandInfobox, ImmutableCollection<ScrapedEpisode>>() {
            public ListenableFuture<ImmutableCollection<ScrapedEpisode>> apply(ScrapedBrandInfobox input) {
                if(input.episodeListLinkTarget == null || input.episodeListLinkTarget.isEmpty()) {
                    ImmutableCollection<ScrapedEpisode> none = ImmutableList.of();
                    return Futures.immediateFuture(none);
                }
                log.debug("Queueing fetch of \""+ input.episodeListLinkTarget +"\" as season list for \""+ input.title +"\"");
                return extractSeasons(fetchMeister.fetchChildArticle(input.episodeListLinkTarget));
            }
        }, executor);

        ListenableFuture<ImmutableCollection<ScrapedEpisode>> mainPageEpisodes = extractSeasons(baseArticle);
        
        ListenableFuture<ImmutableCollection<ScrapedEpisode>> allEpisodes = Futures.transform(Futures.successfulAsList(mainPageEpisodes, externalListedEpisodes), CONCAT);
        
        ListenableFuture<Void> doIt = Futures.transform(new ScrapedFlatHierarchy.Collector(baseArticle, brandInfobox, allEpisodes).collect(), new Function<ScrapedFlatHierarchy, Void>() {
            public Void apply(ScrapedFlatHierarchy input) {
                TvBrandHierarchy extracted = extractor.extract(input);
                writeHierarchy(extracted);
                return null;
            }
        }, executor);
        
        return doIt;
    }
    
    private void writeHierarchy(TvBrandHierarchy hierarchy) {
        log.info("Processing brand  \"" + hierarchy.getBrand() + "\"");
        writer.createOrUpdate(hierarchy.getBrand());
        for (Series s : hierarchy.getSeasons()) {
            writer.createOrUpdate(s);
        }
        for (Episode e : hierarchy.getEpisodes()) {
            log.info("Processing episode article \"" + e.getTitle() + "\"");
            writer.createOrUpdate(e);
        }
    }

}
