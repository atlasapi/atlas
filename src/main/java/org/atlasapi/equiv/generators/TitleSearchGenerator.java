package org.atlasapi.equiv.generators;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import com.metabroadcast.common.query.Selection;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import static org.atlasapi.application.v3.ApplicationConfiguration.defaultConfiguration;

public class TitleSearchGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final static float TITLE_WEIGHTING = 1.0f;
    public final static String NAME = "Title";
    private final Set<String> TO_REMOVE = ImmutableSet.of("rated", "unrated", "(rated)", "(unrated)");
    
    public static final <T extends Content> TitleSearchGenerator<T> create(SearchResolver searchResolver, Class<? extends T> cls, Iterable<Publisher> publishers, double exactMatchScore) {
        return new TitleSearchGenerator<T>(searchResolver, cls, publishers, exactMatchScore);
    }
    
    private final SearchResolver searchResolver;
    private final Class<? extends T> cls;
    private final Set<Publisher> searchPublishers;
    private final Function<String, String> titleTransform;
    private final ContentTitleScorer<T> titleScorer;
    private final int searchLimit;
    private final ExpandingTitleTransformer titleExpander;

    public TitleSearchGenerator(SearchResolver searchResolver, Class<? extends T> cls, Iterable<Publisher> publishers, double exactMatchScore) {
        this(searchResolver, cls, publishers, Functions.<String>identity(), 20, exactMatchScore);
    }
    
    public TitleSearchGenerator(SearchResolver searchResolver, Class<? extends T> cls, Iterable<Publisher> publishers, Function<String,String> titleTransform, int searchLimit,
            double exactMatchScore) {
        this.searchResolver = searchResolver;
        this.cls = cls;
        this.searchLimit = searchLimit;
        this.searchPublishers = ImmutableSet.copyOf(publishers);
        this.titleTransform = titleTransform;
        this.titleScorer = new ContentTitleScorer<T>(NAME, titleTransform, exactMatchScore);
        this.titleExpander = new ExpandingTitleTransformer();
    }

    @Override
    public ScoredCandidates<T> generate(T content, ResultDescription desc) {
        if (Strings.isNullOrEmpty(content.getTitle())) {
            desc.appendText("subject has no title");
            return DefaultScoredCandidates.<T>fromSource(NAME).build();
        }
        Iterable<? extends T> candidates = searchForCandidates(content, desc);
        return titleScorer.scoreCandidates(content, candidates, desc);
    }

    private Iterable<? extends T> searchForCandidates(T content, ResultDescription desc) {
        Set<Publisher> publishers = Sets.difference(searchPublishers, ImmutableSet.of(content.getPublisher()));
        ApplicationConfiguration appConfig = defaultConfiguration().withSources(enabledPublishers(publishers));

        String title = titleTransform.apply(content.getTitle());
        title = normalize(title);
        SearchQuery.Builder titleQuery = getSearchQueryBuilder(publishers, title);

        if (content.getSpecialization() != null) {
            titleQuery.withSpecializations(ImmutableSet.of(content.getSpecialization()));
        }

        desc.appendText("query: %s, specialization: %s, publishers: %s",
                title,
                content.getSpecialization(),
                publishers);

        Iterable<? extends T> results = Iterables.filter(searchResolver.search(
                titleQuery.build(),
                appConfig
        ), cls);

        String expandedTitle = titleExpander.expand(title);

        if (!title.toLowerCase().equals(expandedTitle)) {
            SearchQuery.Builder expandedTitleQuery = getSearchQueryBuilder(publishers,
                    expandedTitle);
            if (content.getSpecialization() != null) {
                titleQuery.withSpecializations(ImmutableSet.of(content.getSpecialization()));
            }
            Iterable<? extends T> filteredExpandedTitleResults = Iterables.filter(searchResolver.search(
                    expandedTitleQuery.build(),
                    appConfig
            ), cls);

            results = Iterables.concat(results, filteredExpandedTitleResults);
        }

        return Iterables.filter(results, IS_ACTIVELY_PUBLISHED);
    }

    private SearchQuery.Builder getSearchQueryBuilder(Set<Publisher> publishers,
            String expandedTitle) {
        return SearchQuery.builder(expandedTitle)
                        .withSelection(new Selection(0, searchLimit))
                        .withPublishers(publishers)
                        .withTitleWeighting(TITLE_WEIGHTING);
    }

    private Map<Publisher, SourceStatus> enabledPublishers(Set<Publisher> enabledSources) {
        ImmutableMap.Builder<Publisher, SourceStatus> builder = ImmutableMap.builder();
        for (Publisher publisher : Publisher.values()) {
            if (enabledSources.contains(publisher)) {
                builder.put(publisher, SourceStatus.AVAILABLE_ENABLED);
            } else {
                builder.put(publisher, SourceStatus.AVAILABLE_DISABLED);
            }
        }
        return builder.build();
    }

    private String normalize(String title) {
        for (String removed : TO_REMOVE) {
            title = title.toLowerCase().replaceAll(removed, "");
        }
        return title;
    }
    
    @Override
    public String toString() {
        return "Title-matching Generator";
    }

    private static Predicate<Content> IS_ACTIVELY_PUBLISHED = new Predicate<Content>() {

        @Override
        public boolean apply(Content input) {
            return input.isActivelyPublished();
        }
    };
    
}
