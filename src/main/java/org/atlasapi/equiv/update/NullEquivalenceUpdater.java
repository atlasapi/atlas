package org.atlasapi.equiv.update;

import java.util.Optional;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public class NullEquivalenceUpdater<T> implements EquivalenceUpdater<T> {

    private enum NullUpdater implements EquivalenceUpdater<Object> {
        INSTANCE {
            @Override
            public boolean updateEquivalences(Object content) {
                return false;
            }

            @Override
            public boolean updateEquivalencesWithReporting(Object subject, Optional<String> taskId,
                    IngestTelescopeClientImpl telescopeClient) {
                return false;
            }
        };

        @SuppressWarnings("unchecked")
        <T> EquivalenceUpdater<T> withNarrowedType() {
            return (EquivalenceUpdater<T>) this;
        }
    }

    public static final <T> EquivalenceUpdater<T> get() {
        return NullUpdater.INSTANCE.withNarrowedType();
    }

    private NullEquivalenceUpdater() {
    }

    @Override
    public boolean updateEquivalences(T content) {
        return false;
    }

    @Override
    public boolean updateEquivalencesWithReporting(T subject, Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient) {
        return updateEquivalences(subject);
    }
}
