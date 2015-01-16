package org.atlasapi.remotesite.getty;

import java.util.Map;

import org.atlasapi.remotesite.metabroadcast.SchedulingStore;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public interface RestartStatusSupplier {
    Optional<Integer> startFromOffset();
    void saveCurrentOffset(Integer offset);
    void clearCurrentOffset();

    public static final class StoreProvidedStatus implements RestartStatusSupplier {
        private static final String KEY_START_FROM_OFFSET = "startFromOffset";

        private final SchedulingStore store;
        public StoreProvidedStatus(SchedulingStore store) {
            this.store = store;
        }

        @Override
        public Optional<Integer> startFromOffset() {
            Optional<Map<String, Object>> retrievedState = store.retrieveState(GettyUpdateTask.JOB_KEY);
            if (retrievedState.isPresent()) {
                return Optional.fromNullable((Integer) retrievedState.get().get(KEY_START_FROM_OFFSET));
            }
            return Optional.absent();
        }

        public void saveCurrentOffset(Integer offset) {
            store.storeState(GettyUpdateTask.JOB_KEY,
                offset != null
                    ? ImmutableMap.of(KEY_START_FROM_OFFSET, (Object) offset)
                    : ImmutableMap.<String, Object>of() );
        }

        public void clearCurrentOffset() {
            saveCurrentOffset(null);
        }
    }
}
