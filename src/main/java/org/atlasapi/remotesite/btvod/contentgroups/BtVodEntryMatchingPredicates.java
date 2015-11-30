package org.atlasapi.remotesite.btvod.contentgroups;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.atlasapi.remotesite.btvod.BtMpxVodClient;
import org.atlasapi.remotesite.btvod.BtVodEntryMatchingPredicate;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

public class BtVodEntryMatchingPredicates {

    private static final Logger log = LoggerFactory.getLogger(BtVodEntryMatchingPredicates.class);

    public static BtVodEntryMatchingPredicate schedulerChannelPredicate(final String schedulerChannel) {
        return new BtVodEntryMatchingPredicate() {
            @Override
            public boolean apply(BtVodEntry input) {
                return schedulerChannel.equals(input.getSchedulerChannel());
            }

            @Override
            public void init() {}
        };
    }

    public static BtVodEntryMatchingPredicate schedulerChannelAndOfferingTypePredicate(
            final String schedulerChannel, final Set<String> productOfferingTypes) {
        return new BtVodEntryMatchingPredicate() {
            @Override
            public boolean apply(BtVodEntry input) {
                return schedulerChannel.equals(input.getSchedulerChannel())
                        && productOfferingTypes.contains(
                        Strings.nullToEmpty(input.getProductOfferingType()).toLowerCase()
                );
            }

            @Override
            public void init() {}
        };
    }

    public static BtVodEntryMatchingPredicate contentProviderPredicate(final String providerId) {
        return new BtVodEntryMatchingPredicate() {
            @Override
            public boolean apply(BtVodEntry input) {
                return providerId.equals(input.getContentProviderId());
            }

            @Override
            public void init() {}
        };
    }

    public static BtVodEntryMatchingPredicate mpxFeedContentMatchingPredicate(final BtMpxVodClient mpxClient, final String feedName) {

        return new BtVodEntryMatchingPredicate() {

            private Set<String> ids = null;

            @Override
            public boolean apply(BtVodEntry input) {
                if (ids == null) {
                    throw new IllegalStateException("Must call init() first");
                }
                log.debug("MPX content group predicate testing whether {} is in group", input.getId());
                return ids.contains(input.getId());
            }

            @Override
            public void init() {
                ImmutableSet.Builder<String> builder = ImmutableSet.builder();
                Iterator<BtVodEntry> feed;
                try {
                    feed = mpxClient.getFeed(feedName);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                while (feed.hasNext()) {
                    builder.add(feed.next().getId());
                };
                ids = builder.build();
                log.debug("MPX content group predicate initialized with IDs {}", ids);
            }
        };
    }

}
