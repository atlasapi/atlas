package org.atlasapi.equiv.generators.amazon;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;

import java.util.Collection;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An exact title generator backed by a dedicated database index for amazon content
 */
public class AmazonTitleGenerator<T extends Content> implements EquivalenceGenerator<T> {

    private final AmazonTitleIndexStore amazonTitleIndexStore;
    private final ContentResolver resolver;
    private final Class<? extends T> cls;
    private final Set<Publisher> publishers;
    private final boolean onlyIncludeTopLevelContent;


    public AmazonTitleGenerator(
            AmazonTitleIndexStore amazonTitleIndexStore,
            ContentResolver resolver,
            Class<? extends T> cls,
            boolean onlyIncludeTopLevelContent,
            Publisher... publishers
    ) {
        this.amazonTitleIndexStore = checkNotNull(amazonTitleIndexStore);
        this.resolver = checkNotNull(resolver);
        this.cls = checkNotNull(cls);
        this.onlyIncludeTopLevelContent = onlyIncludeTopLevelContent;
        this.publishers = ImmutableSet.copyOf(publishers);
    }

    @Override
    public ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Amazon Title Generator");

        AmazonTitleIndexEntry indexEntry = amazonTitleIndexStore.getIndexEntry(subject.getTitle());

        DefaultScoredCandidates.Builder<T> scoredCandidates = DefaultScoredCandidates.fromSource("Amazon Title");

        ResolvedContent resolved = resolver.findByCanonicalUris(indexEntry.getUris());

        Collection<T> filteredContent = resolved.getAllResolvedResults().stream()
                .filter(identified -> !identified.getCanonicalUri().equals(subject.getCanonicalUri()))
                .filter(cls::isInstance)
                .map(cls::cast)
                .filter(content -> publishers.contains(content.getPublisher()))
                .filter(content -> !onlyIncludeTopLevelContent || topLevelContent(content))
                .filter(content -> content.getTitle().equals(subject.getTitle())) //shouldn't be needed but included to be safe
                .collect(MoreCollectors.toImmutableList());

        desc.appendText("%d candidates found for title: %s", filteredContent.size(), subject.getTitle());

        for(T content : filteredContent) {
            desc.appendText("Candidate: %s", content.getCanonicalUri());
            scoredCandidates.addEquivalent(content, Score.nullScore());
        }
        return scoredCandidates.build();
    }

    private boolean topLevelContent(Content content) {
        if(content instanceof Item) {
            if (content instanceof Episode) {
                return false;
            }
            if (((Item) content).getContainer() != null) {
                return false;
            }
        } else if(content instanceof Container) {
            if (content instanceof Series) {
                if (((Series) content).getParent() != null) {
                    return false;
                }
            }
        }
        return true;
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
