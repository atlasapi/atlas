package org.atlasapi.remotesite.btvod;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Predicate;

public interface BtVodEntryMatchingPredicate extends Predicate<BtVodEntry> {

    void init();
}
