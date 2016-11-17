package org.atlasapi.equiv.update;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;

import java.util.Optional;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public class NullEquivalenceUpdater<T> implements EquivalenceUpdater<T> {

    private enum NullUpdater implements EquivalenceUpdater<Object> {
        INSTANCE {
            @Override
            public boolean updateEquivalences(
                    Object content,
                    Optional<String> taskId,
                    IngestTelescopeClientImpl telescopeClient
            ) {
                return false;
            }

            @Override
            public EquivalenceUpdaterMetadata getMetadata() {
                return NopEquivalenceUpdaterMetadata.create();
            }
        };

        @SuppressWarnings("unchecked")
        <T> EquivalenceUpdater<T> withNarrowedType() {
            return (EquivalenceUpdater<T>) this;
        }
    }

    private NullEquivalenceUpdater() {
    }

    public static <T> EquivalenceUpdater<T> get() {
        return NullUpdater.INSTANCE.withNarrowedType();
    }

    @Override
    public boolean updateEquivalences(
            T content,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        return false;
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata() {
        return NopEquivalenceUpdaterMetadata.create();
    }
}
