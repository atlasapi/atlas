package org.atlasapi.equiv.update;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public class SourceSpecificEquivalenceUpdater implements EquivalenceUpdater<Content> {

    public static Builder builder(Publisher source) {
        return new Builder(source);
    }
    
    public static final class Builder {
        
        private final Publisher source;
        private EquivalenceUpdater<Container> topLevelContainer;
        private EquivalenceUpdater<Container> nonTopLevelContainer;
        private EquivalenceUpdater<Item> item;

        public Builder(Publisher source) {
            this.source = source;
        }

        public Builder withTopLevelContainerUpdater(EquivalenceUpdater<Container> updater) {
            this.topLevelContainer = updater;
            return this;
        }
        
        public Builder withNonTopLevelContainerUpdater(EquivalenceUpdater<Container> updater) {
            this.nonTopLevelContainer = updater;
            return this;
        }
        
        public Builder withItemUpdater(EquivalenceUpdater<Item> updater) {
            this.item = updater;
            return this;
        }
        
        public SourceSpecificEquivalenceUpdater build() {
            return new SourceSpecificEquivalenceUpdater(source, topLevelContainer, nonTopLevelContainer, item);
        }

    }

    private final Publisher source;
    private final EquivalenceUpdater<Container> topLevelContainerUpdater;
    private final EquivalenceUpdater<Container> nonTopLevelContainerUpdater;
    private final EquivalenceUpdater<Item> itemUpdater;
    
    public SourceSpecificEquivalenceUpdater(Publisher source,
            EquivalenceUpdater<Container> topLevelContainer,
            EquivalenceUpdater<Container> nonTopLevelContainer, EquivalenceUpdater<Item> item) {
        this.source = source;
        this.topLevelContainerUpdater = topLevelContainer;
        this.nonTopLevelContainerUpdater = nonTopLevelContainer;
        this.itemUpdater = item;
    }

    @Override
    public boolean updateEquivalences(Content content) {
        checkArgument(content.getPublisher().equals(source),"%s can't update data for %s", source, content.getPublisher());
        if (content instanceof Item) {
            return update(itemUpdater, (Item) content);
        } else if (content instanceof Brand) {
            return update(topLevelContainerUpdater, (Container) content);
        } else if (topLevelSeries(content)) {
            return update(topLevelContainerUpdater, (Container) content);
        } else if (!topLevelSeries(content)) {
            return update(nonTopLevelContainerUpdater, (Container) content);
        } else {
            throw new IllegalStateException(String.format("No updater for %s for %s", source, content));
        }
    }

    @Override
    public boolean updateEquivalencesWithReporting(Content content, Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient) {
        if (content instanceof Item) {
            return updateWithReporting(itemUpdater, (Item) content, taskId, telescopeClient);
        } else if (content instanceof Brand) {
            return updateWithReporting(topLevelContainerUpdater, (Container) content, taskId, telescopeClient);
        } else if (topLevelSeries(content)) {
            return updateWithReporting(topLevelContainerUpdater, (Container) content, taskId, telescopeClient);
        } else if (!topLevelSeries(content)) {
            return updateWithReporting(nonTopLevelContainerUpdater, (Container) content, taskId, telescopeClient);
        } else {
            throw new IllegalStateException(String.format("No updater for %s for %s", source, content));
        }
    }

    private boolean topLevelSeries(Content content) {
        return content instanceof Series
            && ((Series)content).getParent() == null;
    }

    private <T> boolean update(EquivalenceUpdater<T> updater,
            T content) {
        checkNotNull(updater, "No updater for %s %s", source, content);
        return updater.updateEquivalences(content);
    }

    private <T> boolean updateWithReporting(
            EquivalenceUpdater<T> updater,
            T content,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        checkNotNull(updater, "No updater for %s %s", source, content);
        return updater.updateEquivalencesWithReporting(
                content,
                taskId,
                telescopeClient
        );
    }
}