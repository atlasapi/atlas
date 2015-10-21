package org.atlasapi.remotesite.wikipedia;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.wikipedia.film.FilmArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.atlasapi.remotesite.wikipedia.television.TvBrandArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.television.TvBrandHierarchyExtractor;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class WikipediaModule {
    private static final Logger log = LoggerFactory.getLogger(WikipediaModule.class);
    static final Publisher SOURCE = Publisher.WIKIPEDIA;

    private @Autowired SimpleScheduler scheduler;
    private @Value("${updaters.wikipedia.films.enabled}") Boolean filmTaskEnabled;
    private @Value("${updaters.wikipedia.tv.enabled}") Boolean tvTaskEnabled;
    
    private @Value("${updaters.wikipedia.films.simultaneousness}") int filmsSimultaneousness;
    private @Value("${updaters.wikipedia.films.threads}") int filmsThreads;
    private @Value("${updaters.wikipedia.tv.simultaneousness}") int tvSimultaneousness;
    private @Value("${updaters.wikipedia.tv.threads}") int tvThreads;

	private @Autowired @Qualifier("contentResolver") ContentResolver contentResolver;
	private @Autowired @Qualifier("contentWriter") ContentWriter contentWriter;
    
    private final EnglishWikipediaClient ewc = new EnglishWikipediaClient();
    protected final ArticleFetcher fetcher = ewc;
    protected final FetchMeister fetchMeister = new FetchMeister(fetcher);
    
    protected final FilmExtractor filmExtractor = new FilmExtractor();
    protected final FilmArticleTitleSource allFilmsTitleSource = ewc;
    
    protected final TvBrandHierarchyExtractor tvBrandHierarchyExtractor = new TvBrandHierarchyExtractor();
    protected final TvBrandArticleTitleSource allTvBrandsTitleSource = ewc;
    
    @PostConstruct
    public void setUp() {
        if(filmTaskEnabled) {
            scheduler.schedule(allFilmsUpdater().withName("Wikipedia films updater"), RepetitionRules.daily(new LocalTime(4,0,0)));
            log.info("Wikipedia film update scheduled task installed");
        }
        if(tvTaskEnabled) {
            scheduler.schedule(allTvBrandsUpdater().withName("Wikipedia TV updater"), RepetitionRules.daily(new LocalTime(4,0,0)));
            log.info("Wikipedia TV update scheduled task installed");
        }
    }
    
    @Bean
    public WikipediaUpdatesController updatesController() {
        return new WikipediaUpdatesController(this);
    }
    
    public FilmsUpdater allFilmsUpdater() {
        return new FilmsUpdater(allFilmsTitleSource, fetchMeister, filmExtractor, contentWriter, filmsSimultaneousness, filmsThreads);
    }
    
    public FilmsUpdater filmsUpdaterForTitles(FilmArticleTitleSource titleSource) {
        return new FilmsUpdater(titleSource, fetchMeister, filmExtractor, contentWriter, filmsSimultaneousness, filmsThreads);
    }
    
    public TvBrandHierarchyUpdater allTvBrandsUpdater() {
        return new TvBrandHierarchyUpdater(allTvBrandsTitleSource, fetchMeister, tvBrandHierarchyExtractor, contentWriter, tvSimultaneousness, tvThreads);
    }
    
    public TvBrandHierarchyUpdater tvBrandsUpdaterForTitles(TvBrandArticleTitleSource titleSource) {
        return new TvBrandHierarchyUpdater(titleSource, fetchMeister, tvBrandHierarchyExtractor, contentWriter, tvSimultaneousness, tvThreads);
    }
}
