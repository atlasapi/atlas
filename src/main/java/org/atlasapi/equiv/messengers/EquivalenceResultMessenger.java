package org.atlasapi.equiv.messengers;

import org.atlasapi.equiv.results.EquivalenceResult;

public interface EquivalenceResultMessenger<T> {

    void sendMessage(EquivalenceResult<T> result);
}
