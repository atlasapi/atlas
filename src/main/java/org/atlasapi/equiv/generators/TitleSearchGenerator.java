package org.atlasapi.equiv.generators;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Song;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class TitleSearchGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final static float TITLE_WEIGHTING = 1.0f;
    private final Set<String> TO_REMOVE = ImmutableSet.of("rated", "unrated", "(rated)", "(unrated)");

    public static final <T extends Content> TitleSearchGenerator<T> create(
            SearchResolver searchResolver,
            Class<? extends T> cls,
            Iterable<Publisher> publishers,
            double exactMatchScore
    ) {
        return create(searchResolver, cls, publishers, exactMatchScore, false);
    }

    public static final <T extends Content> TitleSearchGenerator<T> create(
            SearchResolver searchResolver,
            Class<? extends T> cls,
            Iterable<Publisher> publishers,
            double exactMatchScore,
            boolean includeSelfPublisher
    ) {
        return create(searchResolver, cls, publishers, exactMatchScore, includeSelfPublisher, true);
    }

    public static final <T extends Content> TitleSearchGenerator<T> create(
            SearchResolver searchResolver, Class<? extends T> cls,
            Iterable<Publisher> publishers,
            double exactMatchScore,
            boolean includeSelfPublisher,
            boolean useContentSpecialization
    ) {
        return new TitleSearchGenerator<T>(
                searchResolver,
                cls,
                publishers,
                Functions.<String>identity(),
                20,
                Score.valueOf(exactMatchScore),
                Score.ONE,
                includeSelfPublisher,
                useContentSpecialization,
                false
        );
    }

    public static final <T extends Content> TitleSearchGenerator<T> create(
            SearchResolver searchResolver, Class<? extends T> cls, Set<Publisher> publishers, Score exactMatchScore,
            Score partialMatchBound, boolean includeSelfPublisher, boolean useContentSpecialization, boolean changeName
    ) {
        return new TitleSearchGenerator<T>(
                searchResolver,
                cls,
                publishers,
                Functions.<String>identity(),
                20,
                exactMatchScore,
                partialMatchBound,
                includeSelfPublisher,
                useContentSpecialization,
                changeName  //if false, this will have the same name as the TitleMatching scorer
        );
    }
    
    private final SearchResolver searchResolver;
    private final Class<? extends T> cls;
    private final Set<Publisher> searchPublishers;
    private final Function<String, String> titleTransform;
    private final String name;
    private final ContentTitleScorer<T> titleScorer;
    private final int searchLimit;
    private final ExpandingTitleTransformer titleExpander;
    //include stuff from the same publisher as the given content
    private final boolean includeSelfPublisher;
    //filter search results to same specialization as the given content
    private final boolean useContentSpecialization;

    public TitleSearchGenerator(
            SearchResolver searchResolver, Class<? extends T> cls,
            Iterable<Publisher> publishers,
            Function<String,String> titleTransform,
            int searchLimit,
            Score exactMatchScore,
            Score partialMatchBound,
            boolean includeSelfPublisher,
            boolean useContentSpecialization,
            boolean changeName
    ) {
        this.searchResolver = searchResolver;
        this.cls = cls;
        this.searchLimit = searchLimit;
        this.searchPublishers = ImmutableSet.copyOf(publishers);
        this.titleTransform = titleTransform;
        this.name = changeName ?
                    "Title Search Generator"
                    : "Title";  // same as TitleMatching[...]Scorer
        this.titleScorer = new ContentTitleScorer<>(
                name,
                titleTransform,
                exactMatchScore,
                partialMatchBound
        );
        this.titleExpander = new ExpandingTitleTransformer();
        this.includeSelfPublisher = includeSelfPublisher;
        this.useContentSpecialization = useContentSpecialization;
    }

    @Override
    public ScoredCandidates<T> generate(
            T content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Title Search Generator");

        if (Strings.isNullOrEmpty(content.getTitle())) {
            desc.appendText("subject has no title");
            return DefaultScoredCandidates.<T>fromSource(name).build();
        }
        Iterable<? extends T> candidates = searchForCandidates(content, desc);
        return titleScorer.scoreCandidates(content, candidates, desc, generatorComponent);
    }

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                searchPublishers
        );
    }

    private Iterable<? extends T> searchForCandidates(T content, ResultDescription desc) {
        Set<Publisher> publishers;
        if (includeSelfPublisher) {
            publishers = searchPublishers;
        } else {
            publishers = Sets.difference(
                    searchPublishers, ImmutableSet.of(content.getPublisher())
            );
        }
        Application application = DefaultApplication.createWithReads(Lists.newArrayList(
                publishers));

        String title = titleTransform.apply(content.getTitle());
        title = normalize(title);
        SearchQuery.Builder titleQuery = getSearchQueryBuilder(publishers, title);

        if (useContentSpecialization && content.getSpecialization() != null) {
            titleQuery.withSpecializations(ImmutableSet.of(content.getSpecialization()));
        } else if (!useContentSpecialization) {
            titleQuery.withFakeSpecialization(true);
        }

        desc.appendText("query: %s, specialization: %s, publishers: %s",
                title,
                useContentSpecialization ? content.getSpecialization() : "no specialization",
                publishers);

        Iterable<? extends T> results = searchResolver.search(titleQuery.build(), application)
                .stream()
                .filter(cls::isInstance)
                .map(cls::cast)
                .collect(MoreCollectors.toImmutableList());

        String expandedTitle = titleExpander.expand(title);

        if (!title.equalsIgnoreCase(expandedTitle)) {
            SearchQuery.Builder expandedTitleQuery = getSearchQueryBuilder(
                    publishers,
                    expandedTitle
            );
            if (useContentSpecialization && content.getSpecialization() != null) {
                titleQuery.withSpecializations(ImmutableSet.of(content.getSpecialization()));
            } else if (!useContentSpecialization) {
                titleQuery.withFakeSpecialization(true);
            }
            Iterable<? extends T> filteredExpandedTitleResults = Iterables.filter(
                    searchResolver.search(expandedTitleQuery.build(), application),
                    cls
            );

            results = Iterables.concat(results, filteredExpandedTitleResults);
        }

        //Return actively published results, and not the subject itself.
        return StreamSupport.stream(results.spliterator(), false)
                .filter(Described::isActivelyPublished)
                .filter(input-> !Objects.equals(input.getId(), content.getId()))
                .collect(Collectors.toList());
    }

    private SearchQuery.Builder getSearchQueryBuilder(Set<Publisher> publishers,
            String expandedTitle) {
        return SearchQuery.builder(expandedTitle)
                        .withSelection(new Selection(0, searchLimit))
                        .withPublishers(publishers)
                        .withTitleWeighting(TITLE_WEIGHTING);
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
}
