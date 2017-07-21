package org.atlasapi.remotesite.youview;

import nu.xom.Element;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

public interface YouViewElementProcessor {
    
    Item process(Publisher targetPublisher, Element element);
}
