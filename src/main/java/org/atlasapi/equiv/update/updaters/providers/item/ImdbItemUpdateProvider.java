package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
import org.atlasapi.equiv.generators.ContainerCandidatesItemEquivalenceGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmAndEpisodeFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.ItemYearScorer;
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 *  Equivs on both exact title + exact year match, but not only one or the other.
 *  Also equivs if IMDb alias present, or through sequence stitching via containers.
 */
public class ImdbItemUpdateProvider implements EquivalenceResultUpdaterProvider<Item> {

    private final String IMDB_NAMESPACE = "imdb:id";
    private final String OLD_IMDB_NAMESPACE = "gb:imdb:resourceId";
    private final String AMAZON_IMDB_NAMESPACE = "zz:imdb:id";
    private final String JUSTWATCH_IMDB_NAMESPACE = "justwatch:imdb:id";
    private final Set<Set<String>> NAMESPACES_SET = ImmutableSet.of(
            ImmutableSet.of(IMDB_NAMESPACE, OLD_IMDB_NAMESPACE, AMAZON_IMDB_NAMESPACE, JUSTWATCH_IMDB_NAMESPACE)
    );

    private ImdbItemUpdateProvider() {

    }

    public static ImdbItemUpdateProvider create() {
        return new ImdbItemUpdateProvider();
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
                        ImmutableSet.of(
                                AliasResolvingEquivalenceGenerator.<Item>builder()
                                    .withResolver(dependencies.getContentResolver())
                                    .withPublishers(targetPublishers)
                                    .withLookupEntryStore(dependencies.getLookupEntryStore())
                                    .withNamespacesSet(NAMESPACES_SET)
                                    .withAliasMatchingScore(Score.valueOf(3D))
                                    .withIncludeUnpublishedContent(false)
                                    .withClass(Item.class)
                                    .build(),
                                new ContainerCandidatesItemEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        targetPublishers
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new SequenceItemScorer(Score.valueOf(3D)),
                                new TitleMatchingItemScorer(), // Scores 2 on exact match
                                new ItemYearScorer(Score.ONE, Score.ZERO, Score.nullScore())
                        )
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(2.9),
                                new MediaTypeFilter<>(),
                                new FilmAndEpisodeFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                )
                        ))
                )
                .withExtractor(
                        AllOverOrEqThresholdExtractor.create(3)
                )
                .build();
    }
}
