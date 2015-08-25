package org.atlasapi.remotesite.btvod;

import com.google.common.base.Predicate;


public interface BtVodContentMatchingPredicate extends Predicate<VodEntryAndContent> {

    void init();
}
