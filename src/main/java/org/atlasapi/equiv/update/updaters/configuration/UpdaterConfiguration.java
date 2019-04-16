package org.atlasapi.equiv.update.updaters.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.update.handlers.types.ContainerEquivalenceHandlerType;
import org.atlasapi.equiv.update.handlers.types.ItemEquivalenceHandlerType;
import org.atlasapi.equiv.update.messagers.types.ContainerEquivalenceMessengerType;
import org.atlasapi.equiv.update.messagers.types.ItemEquivalenceMessengerType;
import org.atlasapi.equiv.update.updaters.types.ContainerEquivalenceUpdaterType;
import org.atlasapi.equiv.update.updaters.types.ItemEquivalenceUpdaterType;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpdaterConfiguration {

    private final Publisher source;

    private final ImmutableMap<ItemEquivalenceUpdaterType, ImmutableSet<Publisher>> itemEquivalenceUpdaters;
    private final ItemEquivalenceHandlerType itemEquivalenceHandlerType;
    private final ItemEquivalenceMessengerType itemEquivalenceMessengerType;

    private final ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> topLevelContainerEquivalenceUpdaters;
    private final ContainerEquivalenceHandlerType topLevelContainerEquivalenceHandlerType;
    private final ContainerEquivalenceMessengerType topLevelContainerEquivalenceMessengerType;

    private final ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> nonTopLevelContainerEquivalenceUpdaters;
    private final ContainerEquivalenceHandlerType nonTopLevelContainerEquivalenceHandlerType;
    private final ContainerEquivalenceMessengerType nonTopLevelContainerEquivalenceMessengerType;

    private UpdaterConfiguration(
            Publisher source,
            ImmutableMap<ItemEquivalenceUpdaterType, ImmutableSet<Publisher>> itemEquivalenceUpdaters,
            ItemEquivalenceHandlerType itemEquivalenceHandlerType,
            ItemEquivalenceMessengerType itemEquivalenceMessengerType,
            ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> topLevelContainerEquivalenceUpdaters,
            ContainerEquivalenceHandlerType topLevelContainerEquivalenceHandlerType,
            ContainerEquivalenceMessengerType topLevelContainerEquivalenceMessengerType,
            ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> nonTopLevelContainerEquivalenceUpdaters,
            ContainerEquivalenceHandlerType nonTopLevelContainerEquivalenceHandlerType,
            ContainerEquivalenceMessengerType nonTopLevelContainerEquivalenceMessengerType
    ) {
        this.source = checkNotNull(source);
        this.itemEquivalenceUpdaters = ImmutableMap.copyOf(itemEquivalenceUpdaters);
        this.itemEquivalenceHandlerType = checkNotNull(itemEquivalenceHandlerType);
        this.itemEquivalenceMessengerType = checkNotNull(itemEquivalenceMessengerType);
        this.topLevelContainerEquivalenceUpdaters = ImmutableMap.copyOf(topLevelContainerEquivalenceUpdaters);
        this.topLevelContainerEquivalenceHandlerType = checkNotNull(topLevelContainerEquivalenceHandlerType);
        this.topLevelContainerEquivalenceMessengerType = checkNotNull(topLevelContainerEquivalenceMessengerType);
        this.nonTopLevelContainerEquivalenceUpdaters = ImmutableMap.copyOf(nonTopLevelContainerEquivalenceUpdaters);
        this.nonTopLevelContainerEquivalenceHandlerType = checkNotNull(nonTopLevelContainerEquivalenceHandlerType);
        this.nonTopLevelContainerEquivalenceMessengerType = checkNotNull(nonTopLevelContainerEquivalenceMessengerType);
    }

    public static SourceStep builder() {
        return new Builder();
    }

    public Publisher getSource() {
        return source;
    }

    public ImmutableMap<ItemEquivalenceUpdaterType, ImmutableSet<Publisher>> getItemEquivalenceUpdaters() {
        return itemEquivalenceUpdaters;
    }

    public ItemEquivalenceHandlerType getItemEquivalenceHandlerType() {
        return itemEquivalenceHandlerType;
    }

    public ItemEquivalenceMessengerType getItemEquivalenceMessengerType() {
        return itemEquivalenceMessengerType;
    }

    public ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> getTopLevelContainerEquivalenceUpdaters() {
        return topLevelContainerEquivalenceUpdaters;
    }

    public ContainerEquivalenceHandlerType getTopLevelContainerEquivalenceHandlerType() {
        return topLevelContainerEquivalenceHandlerType;
    }

    public ContainerEquivalenceMessengerType getTopLevelContainerEquivalenceMessengerType() {
        return topLevelContainerEquivalenceMessengerType;
    }

    public ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> getNonTopLevelContainerEquivalenceUpdaters() {
        return nonTopLevelContainerEquivalenceUpdaters;
    }

    public ContainerEquivalenceHandlerType getNonTopLevelContainerEquivalenceHandlerType() {
        return nonTopLevelContainerEquivalenceHandlerType;
    }

    public ContainerEquivalenceMessengerType getNonTopLevelContainerEquivalenceMessengerType() {
        return nonTopLevelContainerEquivalenceMessengerType;
    }

    public interface SourceStep {

        ItemEquivalenceUpdaterStep withSource(Publisher source);
    }

    public interface ItemEquivalenceUpdaterStep {

        TopLevelContainerEquivalenceUpdaterStep withItemEquivalenceUpdater(
                ImmutableMap<ItemEquivalenceUpdaterType, ImmutableSet<Publisher>> itemEquivalenceUpdaters,
                ItemEquivalenceHandlerType itemEquivalenceHandlerType,
                ItemEquivalenceMessengerType itemEquivalenceMessengerType
        );
    }

    public interface TopLevelContainerEquivalenceUpdaterStep {

        NonTopLevelContainerEquivalenceUpdaterStep withTopLevelContainerEquivalenceUpdater(
                ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> topLevelContainerEquivalenceUpdaters,
                ContainerEquivalenceHandlerType topLevelContainerEquivalenceHandlerType,
                ContainerEquivalenceMessengerType topLevelContainerEquivalenceMessengerType
        );
    }

    public interface NonTopLevelContainerEquivalenceUpdaterStep {

        BuildStep withNonTopLevelContainerEquivalenceUpdater(
                ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> nonTopLevelContainerEquivalenceUpdaters,
                ContainerEquivalenceHandlerType nonTopLevelContainerEquivalenceHandlerType,
                ContainerEquivalenceMessengerType nonTopLevelContainerEquivalenceMessengerType
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
        private ImmutableMap<ItemEquivalenceUpdaterType, ImmutableSet<Publisher>> itemEquivalenceUpdaters;
        private ItemEquivalenceHandlerType itemEquivalenceHandlerType;
        private ItemEquivalenceMessengerType itemEquivalenceMessengerType;
        private ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> topLevelContainerEquivalenceUpdaters;
        private ContainerEquivalenceHandlerType topLevelContainerEquivalenceHandlerType;
        private ContainerEquivalenceMessengerType topLevelContainerEquivalenceMessengerType;
        private ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> nonTopLevelContainerEquivalenceUpdaters;
        private ContainerEquivalenceHandlerType nonTopLevelContainerEquivalenceHandlerType;
        private ContainerEquivalenceMessengerType nonTopLevelContainerEquivalenceMessengerType;

        private Builder() {
        }

        @Override
        public ItemEquivalenceUpdaterStep withSource(Publisher source) {
            this.source = source;
            return this;
        }

        @Override
        public TopLevelContainerEquivalenceUpdaterStep withItemEquivalenceUpdater(
                ImmutableMap<ItemEquivalenceUpdaterType, ImmutableSet<Publisher>> itemEquivalenceUpdaters,
                ItemEquivalenceHandlerType itemEquivalenceHandlerType,
                ItemEquivalenceMessengerType itemEquivalenceMessengerType
        ) {
            this.itemEquivalenceUpdaters = itemEquivalenceUpdaters;
            this.itemEquivalenceHandlerType = itemEquivalenceHandlerType;
            this.itemEquivalenceMessengerType = itemEquivalenceMessengerType;
            return this;
        }

        @Override
        public NonTopLevelContainerEquivalenceUpdaterStep withTopLevelContainerEquivalenceUpdater(
                ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> topLevelContainerEquivalenceUpdaters,
                ContainerEquivalenceHandlerType topLevelContainerEquivalenceHandlerType,
                ContainerEquivalenceMessengerType topLevelContainerEquivalenceMessengerType
        ) {
            this.topLevelContainerEquivalenceUpdaters = topLevelContainerEquivalenceUpdaters;
            this.topLevelContainerEquivalenceHandlerType = topLevelContainerEquivalenceHandlerType;
            this.topLevelContainerEquivalenceMessengerType = topLevelContainerEquivalenceMessengerType;
            return this;
        }

        @Override
        public BuildStep withNonTopLevelContainerEquivalenceUpdater(
                ImmutableMap<ContainerEquivalenceUpdaterType, ImmutableSet<Publisher>> nonTopLevelContainerEquivalenceUpdaters,
                ContainerEquivalenceHandlerType nonTopLevelContainerEquivalenceHandlerType,
                ContainerEquivalenceMessengerType nonTopLevelContainerEquivalenceMessengerType
        ) {
            this.nonTopLevelContainerEquivalenceUpdaters = nonTopLevelContainerEquivalenceUpdaters;
            this.nonTopLevelContainerEquivalenceHandlerType = nonTopLevelContainerEquivalenceHandlerType;
            this.nonTopLevelContainerEquivalenceMessengerType = nonTopLevelContainerEquivalenceMessengerType;
            return this;
        }

        @Override
        public UpdaterConfiguration build() {
            return new UpdaterConfiguration(
                    this.source,
                    this.itemEquivalenceUpdaters,
                    this.itemEquivalenceHandlerType,
                    this.itemEquivalenceMessengerType,
                    this.topLevelContainerEquivalenceUpdaters,
                    this.topLevelContainerEquivalenceHandlerType,
                    this.topLevelContainerEquivalenceMessengerType,
                    this.nonTopLevelContainerEquivalenceUpdaters,
                    this.nonTopLevelContainerEquivalenceHandlerType,
                    this.nonTopLevelContainerEquivalenceMessengerType
            );
        }
    }
}
