package org.atlasapi.remotesite.wikipedia;

import com.google.common.collect.ImmutableSet;
import net.sourceforge.jwbf.mediawiki.bots.MediaWikiBot;

import org.atlasapi.remotesite.FetchException;
import org.atlasapi.remotesite.wikipedia.film.FilmArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.football.EnglishTeamListScraper;
import org.atlasapi.remotesite.wikipedia.football.TeamsNamesSource;
import org.atlasapi.remotesite.wikipedia.football.EuropeanTeamListScraper;
import org.atlasapi.remotesite.wikipedia.people.ActorsNamesListScrapper;
import org.atlasapi.remotesite.wikipedia.people.PeopleNamesSource;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.ArticleFetcher;
import org.atlasapi.remotesite.wikipedia.wikiparsers.IndexScraper;
import org.atlasapi.remotesite.wikipedia.wikiparsers.JwbfArticle;
import org.atlasapi.remotesite.wikipedia.television.TvBrandArticleTitleSource;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class EnglishWikipediaClient implements ArticleFetcher, FilmArticleTitleSource, TvBrandArticleTitleSource, TeamsNamesSource , PeopleNamesSource {
    private static final Logger log = LoggerFactory.getLogger(EnglishWikipediaClient.class);
    
    private static final MediaWikiBot bot = new MediaWikiBot("http://en.wikipedia.org/w/");

    private static Iterable<String> filmIndexPageTitles() {
        return Lists.transform(ImmutableList.of(
            "numbers", "A", "B", "C", "D", "E", "F", "G", "H", "I",
            "J–K", "L", "M", "N–O", "P", "Q–R", "S", "T", "U–W", "X–Z"
        ), new Function<String, String>() {
            @Override public String apply(String suffix) {
                return "List of films: " + suffix;
            }
        });
    }
    
    @Override
    public Article fetchArticle(String title) throws FetchFailedException {
        try {
            net.sourceforge.jwbf.core.contentRep.Article article = bot.getArticle(title);
            return new JwbfArticle(article);
        } catch (Exception e) {  // probably an IllegalStateException (if you look far down enough) but it's all unchecked, so who knows...
            throw new FetchException("JWBF reported failure", e);
        }
    }

    @Override
    public ImmutableList<String> getAllFilmArticleTitles() {
        log.info("Loading film article titles");
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for(String indexTitle : filmIndexPageTitles()) {
            try {
                builder.addAll(IndexScraper.extractNames(fetchArticle(indexTitle).getMediaWikiSource()));
            } catch (Exception ex) {
                log.error("Failed to load some of the film article names ("+ indexTitle +") – they'll be skipped!", ex);
            }
        }
        return builder.build();
    }

    @Override
    public Iterable<String> getAllTvBrandArticleTitles() throws TvIndexingException {
        try {
            return IndexScraper.extractNames(fetchArticle("List of television programs by name").getMediaWikiSource());
        } catch (Exception ex) {
            throw new TvIndexingException(ex);
        }
    }

    @Override
    public Iterable<String> getAllTeamNames() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        try {
            builder.addAll(EuropeanTeamListScraper.extractNames(fetchArticle("2015–16 UEFA Champions League group stage").getMediaWikiSource()))
            .addAll(EuropeanTeamListScraper.extractNames(fetchArticle("2015–16 UEFA Champions League qualifying phase and play-off round").getMediaWikiSource()))
            .addAll(EuropeanTeamListScraper.extractNames(fetchArticle("2015–16 UEFA Europa League group stage").getMediaWikiSource()))
            .addAll(EuropeanTeamListScraper.extractNames(fetchArticle("2015–16 UEFA Europa League qualifying phase and play-off round").getMediaWikiSource()))
            .addAll(EnglishTeamListScraper.extractNames(fetchArticle("Premier League").getMediaWikiSource()))
            .addAll(EnglishTeamListScraper.extractNames(fetchArticle("Football League One").getMediaWikiSource()))
            .addAll(EnglishTeamListScraper.extractNames(fetchArticle("Football League Championship").getMediaWikiSource()))
            .addAll(EnglishTeamListScraper.extractNames(fetchArticle("Football League Two").getMediaWikiSource()))
            .addAll(EnglishTeamListScraper.extractNames(fetchArticle("National League (division)").getMediaWikiSource()));
            return builder.build();
        } catch (Exception ex) {
            log.error("Failed to load football teams list!", ex);
            throw new RuntimeException();
        }
    }

    @Override
    public Iterable<String> getAllPeopleNames() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        try {
            builder.addAll(ActorsNamesListScrapper.extractNames("Category:American_male_film_actors"));
            builder.addAll(ActorsNamesListScrapper.extractNames("Category:American_film_actresses"));
            builder.addAll(ActorsNamesListScrapper.extractNames("Category:American_male_television_actors"));
            builder.addAll(ActorsNamesListScrapper.extractNames("Category:American_television_actresses"));
            builder.addAll(IndexScraper.extractNames(fetchArticle("List of British actors and actresses").getMediaWikiSource()));
            return builder.build();
        } catch (Exception ex) {
            log.error("Failed to load people names list!", ex);
            throw new RuntimeException();
        }
    }
}
