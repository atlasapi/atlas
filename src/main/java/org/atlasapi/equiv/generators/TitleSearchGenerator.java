package org.atlasapi.equiv.generators;

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
        title = titleExpander.expand(title);
        SearchQuery.Builder query = SearchQuery.builder(title)
                .withSelection(new Selection(0, searchLimit))
                .withPublishers(publishers)
                .withTitleWeighting(TITLE_WEIGHTING);
        if (content.getSpecialization() != null) {
            query.withSpecializations(ImmutableSet.of(content.getSpecialization()));
        }
        
        desc.appendText("query: %s, specialization: %s, publishers: %s",
                title,
                content.getSpecialization(),
                publishers);

        Iterable<? extends T> search =
                Iterables.filter(searchResolver.search(query.build(), appConfig), cls);

        return Iterables.filter(search, IS_ACTIVELY_PUBLISHED);
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
