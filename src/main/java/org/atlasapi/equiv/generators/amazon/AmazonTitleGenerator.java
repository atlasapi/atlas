package org.atlasapi.equiv.generators.amazon;

import java.util.Collection;
import java.util.Set;

import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.amazon.AmazonContentWritingItemProcessor;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.amazon.AmazonContentWritingItemProcessor.isTopLevelContent;

/**
 * An exact title generator backed by a dedicated database index for amazon content
 */
public class AmazonTitleGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final AmazonTitleIndexStore amazonTitleIndexStore;
    private final ContentResolver resolver;
    private final Class<? extends T> cls;
    private final Set<Publisher> publishers;


    public AmazonTitleGenerator(
            AmazonTitleIndexStore amazonTitleIndexStore,
            ContentResolver resolver,
            Class<? extends T> cls,
            Publisher... publishers
    ) {
        this.amazonTitleIndexStore = checkNotNull(amazonTitleIndexStore);
        this.resolver = checkNotNull(resolver);
        this.cls = checkNotNull(cls);
        this.publishers = ImmutableSet.copyOf(publishers);
    }

    @Override
    public ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResults
    ) {
        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Amazon Title Generator");

        DefaultScoredCandidates.Builder<T> scoredCandidates = DefaultScoredCandidates.fromSource("Amazon Title");

        if (!isTopLevelContent(subject)) {
            desc.appendText("Won't generate: subject not top level content");
            return scoredCandidates.build();
        }

        AmazonTitleIndexEntry indexEntry = amazonTitleIndexStore.getIndexEntry(subject.getTitle());

        ResolvedContent resolved = resolver.findByCanonicalUris(indexEntry.getUris());

        Collection<T> filteredContent = resolved.getAllResolvedResults().stream()
                .filter(identified -> !identified.getCanonicalUri().equals(subject.getCanonicalUri()))
                .filter(cls::isInstance)
                .map(cls::cast)
                .filter(content -> publishers.contains(content.getPublisher()))
                .filter(AmazonContentWritingItemProcessor::isTopLevelContent)
                .filter(content -> content.getTitle().equals(subject.getTitle())) //shouldn't be needed but included to be safe
                .collect(MoreCollectors.toImmutableList());

        desc.appendText("%d candidates found for title: %s", filteredContent.size(), subject.getTitle());

        for(T content : filteredContent) {
            desc.appendText("Candidate: %s", content.getCanonicalUri());
            scoredCandidates.addEquivalent(content, Score.nullScore());
        }
        return scoredCandidates.build();
    }

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                publishers
        );
    }

    @Override
    public String toString() {
        return "Amazon Title Generator";
    }
}
