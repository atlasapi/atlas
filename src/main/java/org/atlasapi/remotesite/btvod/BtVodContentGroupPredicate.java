package org.atlasapi.remotesite.btvod;

import com.google.common.base.Predicate;


public interface BtVodContentGroupPredicate extends Predicate<VodEntryAndContent> {

    void init();
}
