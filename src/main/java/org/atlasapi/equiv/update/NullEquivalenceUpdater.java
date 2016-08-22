package org.atlasapi.equiv.update;

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
    public boolean updateEquivalences(
            T content,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        return false;
    }

}
