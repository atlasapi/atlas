package org.atlasapi.remotesite.channel4.pmlsd;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;

public interface C4ContentWriter {

    Item createOrUpdate(Item item, @Nullable Object entry);

    void createOrUpdate(Container container, @Nullable Object entry);

}