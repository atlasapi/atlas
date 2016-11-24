package org.atlasapi.equiv.update.updaters.configuration;

import org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType;
import org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpdaterConfiguration {

    private final Publisher source;

    private final ItemEquivalenceUpdaterType itemEquivalenceUpdaterType;
    private final ImmutableSet<Publisher> itemEquivalenceTargetSources;

    private final ContainerEquivalenceUpdaterType topLevelContainerEquivalenceUpdaterType;
    private final ImmutableSet<Publisher> topLevelContainerTargetSources;

    private final ContainerEquivalenceUpdaterType nonTopLevelContainerEquivalenceUpdaterType;
    private final ImmutableSet<Publisher> nonTopLevelContainerTargetSources;

    private UpdaterConfiguration(
            Publisher source,
            ItemEquivalenceUpdaterType itemEquivalenceUpdaterType,
            ImmutableSet<Publisher> itemEquivalenceTargetSources,
            ContainerEquivalenceUpdaterType topLevelContainerEquivalenceUpdaterType,
            ImmutableSet<Publisher> topLevelContainerTargetSources,
            ContainerEquivalenceUpdaterType nonTopLevelContainerEquivalenceUpdaterType,
            ImmutableSet<Publisher> nonTopLevelContainerTargetSources
    ) {
        this.source = checkNotNull(source);

        this.itemEquivalenceUpdaterType = checkNotNull(itemEquivalenceUpdaterType);
        this.itemEquivalenceTargetSources = ImmutableSet.copyOf(itemEquivalenceTargetSources);

        this.topLevelContainerEquivalenceUpdaterType = checkNotNull(
                topLevelContainerEquivalenceUpdaterType
        );
        this.topLevelContainerTargetSources = ImmutableSet.copyOf(topLevelContainerTargetSources);

        this.nonTopLevelContainerEquivalenceUpdaterType = checkNotNull(
                nonTopLevelContainerEquivalenceUpdaterType
        );
        this.nonTopLevelContainerTargetSources = ImmutableSet.copyOf(
                nonTopLevelContainerTargetSources
        );
    }

    public static SourceStep builder() {
        return new Builder();
    }

    public Publisher getSource() {
        return source;
    }

    public ItemEquivalenceUpdaterType getItemEquivalenceUpdaterType() {
        return itemEquivalenceUpdaterType;
    }

    public ImmutableSet<Publisher> getItemEquivalenceTargetSources() {
        return itemEquivalenceTargetSources;
    }

    public ContainerEquivalenceUpdaterType getTopLevelContainerEquivalenceUpdaterType() {
        return topLevelContainerEquivalenceUpdaterType;
    }

    public ImmutableSet<Publisher> getTopLevelContainerTargetSources() {
        return topLevelContainerTargetSources;
    }

    public ContainerEquivalenceUpdaterType getNonTopLevelContainerEquivalenceUpdaterType() {
        return nonTopLevelContainerEquivalenceUpdaterType;
    }

    public ImmutableSet<Publisher> getNonTopLevelContainerTargetSources() {
        return nonTopLevelContainerTargetSources;
    }

    public interface SourceStep {

        ItemEquivalenceUpdaterStep withSource(Publisher source);
    }

    public interface ItemEquivalenceUpdaterStep {

        TopLevelContainerEquivalenceUpdaterStep withItemEquivalenceUpdater(
                ItemEquivalenceUpdaterType itemEquivalenceUpdaterType,
                ImmutableSet<Publisher> itemEquivalenceTargetSources
        );
    }

    public interface TopLevelContainerEquivalenceUpdaterStep {

        NonTopLevelContainerEquivalenceUpdaterStep withTopLevelContainerEquivalenceUpdater(
                ContainerEquivalenceUpdaterType topLevelContainerEquivalenceUpdaterType,
                ImmutableSet<Publisher> topLevelContainerTargetSources
        );
    }

    public interface NonTopLevelContainerEquivalenceUpdaterStep {

        BuildStep withNonTopLevelContainerEquivalenceUpdater(
                ContainerEquivalenceUpdaterType nonTopLevelContainerEquivalenceUpdaterType,
                ImmutableSet<Publisher> nonTopLevelContainerTargetSources
        );
    }

    public interface BuildStep {

        UpdaterConfiguration build();
    }

    public static class Builder
            implements SourceStep, ItemEquivalenceUpdaterStep,
            TopLevelContainerEquivalenceUpdaterStep, NonTopLevelContainerEquivalenceUpdaterStep,
            BuildStep {

        private Publisher source;
        private ItemEquivalenceUpdaterType itemEquivalenceUpdaterType;
        private ImmutableSet<Publisher> itemEquivalenceTargetSources;
        private ContainerEquivalenceUpdaterType topLevelContainerEquivalenceUpdaterType;
        private ImmutableSet<Publisher> topLevelContainerTargetSources;
        private ContainerEquivalenceUpdaterType nonTopLevelContainerEquivalenceUpdaterType;
        private ImmutableSet<Publisher> nonTopLevelContainerTargetSources;

        private Builder() {
        }

        @Override
        public ItemEquivalenceUpdaterStep withSource(Publisher source) {
            this.source = source;
            return this;
        }

        @Override
        public TopLevelContainerEquivalenceUpdaterStep withItemEquivalenceUpdater(
                ItemEquivalenceUpdaterType itemEquivalenceUpdaterType,
                ImmutableSet<Publisher> itemEquivalenceTargetSources
        ) {
            this.itemEquivalenceUpdaterType = itemEquivalenceUpdaterType;
            this.itemEquivalenceTargetSources = itemEquivalenceTargetSources;
            return this;
        }

        @Override
        public NonTopLevelContainerEquivalenceUpdaterStep withTopLevelContainerEquivalenceUpdater(
                ContainerEquivalenceUpdaterType topLevelContainerEquivalenceUpdaterType,
                ImmutableSet<Publisher> topLevelContainerTargetSources
        ) {
            this.topLevelContainerEquivalenceUpdaterType = topLevelContainerEquivalenceUpdaterType;
            this.topLevelContainerTargetSources = topLevelContainerTargetSources;
            return this;
        }

        @Override
        public BuildStep withNonTopLevelContainerEquivalenceUpdater(
                ContainerEquivalenceUpdaterType nonTopLevelContainerEquivalenceUpdaterType,
                ImmutableSet<Publisher> nonTopLevelContainerTargetSources
        ) {
            this.nonTopLevelContainerEquivalenceUpdaterType =
                    nonTopLevelContainerEquivalenceUpdaterType;
            this.nonTopLevelContainerTargetSources = topLevelContainerTargetSources;
            return this;
        }

        @Override
        public UpdaterConfiguration build() {
            return new UpdaterConfiguration(
                    this.source,
                    this.itemEquivalenceUpdaterType,
                    this.itemEquivalenceTargetSources,
                    this.topLevelContainerEquivalenceUpdaterType,
                    this.topLevelContainerTargetSources,
                    this.nonTopLevelContainerEquivalenceUpdaterType,
                    this.nonTopLevelContainerTargetSources
            );
        }
    }
}
