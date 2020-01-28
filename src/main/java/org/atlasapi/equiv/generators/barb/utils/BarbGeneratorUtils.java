package org.atlasapi.equiv.generators.barb.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Version;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Set;
import java.util.function.Predicate;

public class BarbGeneratorUtils {
    /**
     * There may be a solution for this that utilises channel equivalence however due to time constraints and a lack of
     * knowledge of channel equivalence I have opted to use a hardcoded map for the time being.
     * <p>
     * N.B. that the MAS file ingest already uses channel equivalence to produce the station-codes.tsv file and so if
     * any other non-barb channel is equived to the barb channel its uri is output in the tsv file and all txlogs for
     * that station code will be ingested on the non-barb channel instead. In the case of txlog BBC2 England regional
     * channels this meant they were all originally being ingested on the single Nitro channel causing most of their
     * broadcasts to become unpublished since the channel can only have one piece of content present in a given time
     * slot. Now they are ingested on their own barb channels and use this map in order to search for candidates from
     * the Nitro channel. If changing to using channel equivalence for candidate generation then the MAS file ingest may
     * need to be changed to make sure broadcasts on txlog content are ingested correctly.
     */
    public static final ImmutableSetMultimap<String, String> CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS =
            ImmutableSetMultimap.<String, String>builder()
                    .putAll(
                            "http://www.bbc.co.uk/services/bbctwo/england", ImmutableSet.of(
                                    "http://channels.barb.co.uk/channels/1081",
                                    "http://channels.barb.co.uk/channels/1082",
                                    "http://channels.barb.co.uk/channels/1083",
                                    "http://channels.barb.co.uk/channels/1084",
                                    "http://channels.barb.co.uk/channels/1085",
                                    "http://channels.barb.co.uk/channels/1086",
                                    "http://channels.barb.co.uk/channels/1087",
                                    "http://channels.barb.co.uk/channels/1088",
                                    "http://channels.barb.co.uk/channels/1093",
                                    "http://channels.barb.co.uk/channels/1094",
                                    "http://channels.barb.co.uk/channels/1095"
                            )
                    )
                    .build();

    public static final Set<String> BBC1_TXLOG_CHANNEL_URIS = ImmutableSet.of(
            "http://www.bbc.co.uk/services/bbcone/east",
            "http://www.bbc.co.uk/services/bbcone/east_midlands",
            "http://www.bbc.co.uk/services/bbcone/east_yorkshire",
            "http://www.bbc.co.uk/services/bbcone/london",
            "http://www.bbc.co.uk/services/bbcone/ni",
            "http://www.bbc.co.uk/services/bbcone/north_east",
            "http://www.bbc.co.uk/services/bbcone/north_west",
            "http://www.bbc.co.uk/services/bbcone/scotland",
            "http://www.bbc.co.uk/services/bbcone/south",
            "http://www.bbc.co.uk/services/bbcone/south_east",
            "http://www.bbc.co.uk/services/bbcone/south_west",
            "http://www.bbc.co.uk/services/bbcone/wales",
            "http://www.bbc.co.uk/services/bbcone/west",
            "http://www.bbc.co.uk/services/bbcone/west_midlands"
    );

    public static final Set<String> BBC2_TXLOG_CHANNEL_URIS = ImmutableSet.of(
            "http://channels.barb.co.uk/channels/1081",
            "http://channels.barb.co.uk/channels/1082",
            "http://channels.barb.co.uk/channels/1083",
            "http://channels.barb.co.uk/channels/1084",
            "http://channels.barb.co.uk/channels/1085",
            "http://channels.barb.co.uk/channels/1086",
            "http://channels.barb.co.uk/channels/1087",
            "http://channels.barb.co.uk/channels/1088",
            "http://channels.barb.co.uk/channels/1093",
            "http://channels.barb.co.uk/channels/1094",
            "http://channels.barb.co.uk/channels/1095",
            "http://www.bbc.co.uk/services/bbctwo/ni",
            "http://www.bbc.co.uk/services/bbctwo/scotland",
            "http://www.bbc.co.uk/services/bbctwo/wales"
    );

    public static Set<String> expandChannelUris(String channelUri) {
        ImmutableSet.Builder<String> channelUris = ImmutableSet.builder();
        channelUris.add(channelUri);
        if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsKey(channelUri)) {
            channelUris.addAll(CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(channelUri));
        } else if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsValue(channelUri)) {
            channelUris.addAll(CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.inverse().get(channelUri));
        }
        return channelUris.build();
    }

    public static boolean sameChannel(String channelUri, String otherChannelUri) {
        if (channelUri.equals(otherChannelUri)) {
            return true;
        }
        if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsKey(channelUri)) {
            if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(channelUri).contains(otherChannelUri)) {
                return true;
            }
        }
        if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.containsKey(otherChannelUri)) {
            if (CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(otherChannelUri).contains(channelUri)) {
                return true;
            }
        }
        return false;
    }

    public static Predicate<Broadcast> minimumDuration(Duration duration) {
        return broadcast -> {
            Duration broadcastDuration = new Duration(
                    broadcast.getTransmissionTime(),
                    broadcast.getTransmissionEndTime()
            );
            return broadcastDuration.isLongerThan(duration);
        };
    }


    public static boolean hasQualifyingBroadcast(
            Item item,
            Broadcast referenceBroadcast,
            Duration flexibility,
            Predicate<? super Broadcast> broadcastFilter
    ) {
        for (Version version : item.nativeVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (around(broadcast, referenceBroadcast, flexibility) && broadcast.getBroadcastOn() != null
                        && sameChannel(broadcast.getBroadcastOn(), referenceBroadcast.getBroadcastOn())
                        && broadcast.isActivelyPublished()
                        && broadcastFilter.test(broadcast)
                ) {
                    return true;
                }
            }
        }
        return false;
    }
    public static boolean hasFlexibleQualifyingBroadcast(
            Item item,
            Broadcast referenceBroadcast,
            Duration flexibility,
            Duration extendedEndTimeFlexibility,
            Predicate<? super Broadcast> broadcastFilter
    ) {
        for (Version version : item.nativeVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (flexibleAround(broadcast, referenceBroadcast, flexibility, extendedEndTimeFlexibility)
                        && broadcast.getBroadcastOn() != null
                        && sameChannel(broadcast.getBroadcastOn(), referenceBroadcast.getBroadcastOn())
                        && broadcast.isActivelyPublished()
                        && broadcastFilter.test(broadcast)
                ) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean around(Broadcast broadcast, Broadcast referenceBroadcast, Duration flexibility) {
        return around(broadcast.getTransmissionTime(), referenceBroadcast.getTransmissionTime(), flexibility)
                && around(broadcast.getTransmissionEndTime(), referenceBroadcast.getTransmissionEndTime(), flexibility);
    }

    public static boolean around(DateTime transmissionTime, DateTime transmissionTime2, Duration flexibility) {
        return !transmissionTime.isBefore(transmissionTime2.minus(flexibility))
                && !transmissionTime.isAfter(transmissionTime2.plus(flexibility));
    }

    public static boolean flexibleAround(
            Broadcast broadcast,
            Broadcast referenceBroadcast,
            Duration flexibility,
            Duration extendedEndTimeFlexibility
    ) {
        return around(
                broadcast.getTransmissionTime(),
                referenceBroadcast.getTransmissionTime(),
                flexibility
        ) && around(
                broadcast.getTransmissionEndTime(),
                referenceBroadcast.getTransmissionEndTime(),
                extendedEndTimeFlexibility
        );
    }
}
