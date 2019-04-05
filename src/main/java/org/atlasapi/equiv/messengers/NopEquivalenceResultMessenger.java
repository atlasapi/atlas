package org.atlasapi.equiv.messengers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;

public class NopEquivalenceResultMessenger<T extends Content> implements EquivalenceResultMessenger<T> {

    public NopEquivalenceResultMessenger() {
    }

    @Override
    public void sendMessage(EquivalenceResult<T> result) {
    }
}
