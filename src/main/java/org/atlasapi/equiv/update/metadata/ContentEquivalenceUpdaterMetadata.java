package org.atlasapi.equiv.update.metadata;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.media.entity.Content;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private final ImmutableList<EquivalenceUpdaterMetadata> equivalenceResultUpdaterMetadata;
    private final String handler;

    private ContentEquivalenceUpdaterMetadata(
            Iterable<EquivalenceUpdaterMetadata> equivalenceResultUpdaterMetadata,
            String handler
    ) {
        this.equivalenceResultUpdaterMetadata = ImmutableList.copyOf(equivalenceResultUpdaterMetadata);
        this.handler = checkNotNull(handler);
    }

    public static EquivalenceResultUpdatersStep builder() {
        return new Builder();
    }

    public ImmutableList<EquivalenceUpdaterMetadata> getEquivalenceResultUpdaterMetadata() {
        return equivalenceResultUpdaterMetadata;
    }

    public String getHandler() {
        return handler;
    }

    public interface EquivalenceResultUpdatersStep {

        <T extends Content> HandlerStep withEquivalenceResultUpdaters(
                Collection<EquivalenceResultUpdater<T>> equivalenceResultUpdaters
        );
    }

    public interface HandlerStep {

        <T> BuildStep withHandler(EquivalenceResultHandler<T> handler);
    }

    public interface BuildStep {

        ContentEquivalenceUpdaterMetadata build();
    }

    public static class Builder implements EquivalenceResultUpdatersStep, HandlerStep, BuildStep {

        private List<EquivalenceUpdaterMetadata> equivalenceResultUpdaterMetadata;
        private String handler;

        private Builder() {
        }

        @Override
        public <T extends Content> HandlerStep withEquivalenceResultUpdaters(
                Collection<EquivalenceResultUpdater<T>> equivalenceResultUpdaters
        ) {
            this.equivalenceResultUpdaterMetadata = equivalenceResultUpdaters.stream()
                    .map(EquivalenceResultUpdater::getMetadata)
                    .collect(MoreCollectors.toImmutableList());
            return this;
        }

        @Override
        public <T> BuildStep withHandler(EquivalenceResultHandler<T> handler) {
            this.handler = getClassName(handler);
            return this;
        }

        @Override
        public ContentEquivalenceUpdaterMetadata build() {
            return new ContentEquivalenceUpdaterMetadata(
                    this.equivalenceResultUpdaterMetadata,
                    this.handler
            );
        }
    }
}
