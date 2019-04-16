package org.atlasapi.equiv.messengers;

import org.atlasapi.equiv.results.EquivalenceResults;

public interface EquivalenceResultMessenger<T> {

    void sendMessage(EquivalenceResults<T> results);
}
