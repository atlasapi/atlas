package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Version;
import org.jdom.Element;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;

public class C4EpgEpisodeExtractor extends BaseC4EpisodeExtractor {
    
    private static final String DC_START_TIME = "dc:relation.TXStartTime";
    private static final String DC_TX_CHANNEL = "dc:relation.TXChannel";

    private static final DateTimeFormatter fmt = DateTimeFormat.forPattern("EEEE dd MMMM yyyy hh:mm aa").withZone(DateTimeZones.LONDON);
    private final Map<String, String> channelLookup;

    public C4EpgEpisodeExtractor(C4AtomApi atomApi, ContentFactory<Feed, Feed, Entry> contentFactory, 
            Clock clock) {
        super(contentFactory, clock);
        this.channelLookup = ImmutableMap.copyOf(
                Maps.transformValues(atomApi.getChannelMap(), Identified.TO_URI));
    }
    
    @Override // epg.atom entries don't have media groups/content, so no images.
    protected Element getMedia(Entry source) {
        return null;
    }
    
    @Override
    protected Episode setAdditionalEpisodeFields(Entry entry, Map<String, String> lookup,
            Episode episode) {
        Version version = new Version();
        episode.setVersions(Sets.newHashSet(version));

        String channelKey = lookup.get(DC_TX_CHANNEL);
        String txChannel = channelLookup.get(channelKey);
        String startTime = lookup.get(DC_START_TIME);
        Duration duration = C4AtomApi.durationFrom(lookup);

        String programmeId = lookup.get(C4AtomApi.DC_PROGRAMME_ID);
        if (programmeId != null) {
            episode.addAlias(new Alias("gb:channel4:prod:pmlsd:programmeId", programmeId));
        }

        if (txChannel != null) {
            DateTime txStart = fmt.parseDateTime(startTime);
            Broadcast broadcast = C4BroadcastBuilder.broadcast()
                    .withChannel(txChannel)
                    .withTransmissionStart(txStart)
                    .withDuration(duration)
                    .withAtomId(entry.getId())
                    .build();
            
            broadcast.setLastUpdated(episode.getLastUpdated());
            version.addBroadcast(broadcast);
        } else {
            throw new IllegalArgumentException(String.format("Channel %s not recognised, key was %s", txChannel, channelKey));
        }

        return episode;
    }

}
