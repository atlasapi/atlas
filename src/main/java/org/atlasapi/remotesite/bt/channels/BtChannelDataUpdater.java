package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.util.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class BtChannelDataUpdater {

    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec();
    private ChannelResolver channelResolver;

    public BtChannelDataUpdater(ChannelResolver channelResolver) {
        this.channelResolver = channelResolver;
    }

    public void addAliasesToChannel(PaginatedEntries paginatedEntries) {

        List<Entry> entries = paginatedEntries.getEntries();

        Set<Long> channelIdsThatHaveAliasesAdded = Sets.newHashSet();

        for(Entry currentEntry : entries) {

            String linearEpgChannelId = currentEntry.getLinearEpgChannelId();
            DateTime advertiseAvailableDate = currentEntry.getAvailableDate();
            long currentGuid = codec.decode(currentEntry.getGuid()).longValue();

            Maybe<Channel> channelMaybe = channelResolver.fromId(currentGuid);

            if(channelMaybe.hasValue()) {
                Channel channel = channelMaybe.requireValue();
                Set<Alias> aliases = channel.getAliases();

                Iterator<Alias> aliasesIterator = aliases.iterator();

                while(aliasesIterator.hasNext()) {

                    Alias alias = aliasesIterator.next();

                    if (alias.getNamespace().equals(linearEpgChannelId)) {
                        aliasesIterator.remove();
                    }

                }

                //Using "namespace" as namespace for now.
                channel.addAlias(new Alias("namespace", linearEpgChannelId));

                channelIdsThatHaveAliasesAdded.add(currentGuid);
            }
        }

        removeStaleAliasesFromChannel(channelIdsThatHaveAliasesAdded, channelResolver.all());
    }

    public void addAvailableDateToChannel(PaginatedEntries paginatedEntries) {
        List<Entry> entries = paginatedEntries.getEntries();

        Set<Long> channelIdsThatHaveAvailableDateAdded = Sets.newHashSet();

        for(Entry currentEntry : entries) {

            DateTime advertiseAvailableDate = currentEntry.getAvailableDate();
            long currentGuid = codec.decode(currentEntry.getGuid()).longValue();

            Maybe<Channel> channelMaybe = channelResolver.fromId(currentGuid);

            if(channelMaybe.hasValue()) {
                Channel channel = channelMaybe.requireValue();

                //Turned off for now until it is added to atlas model and atlas deer. Did a git pull but couldn't get dias' changes probably because it is only on stage. (my branch is based off master)
                //(advertiseAvailableDate != null && advertiseAvailableDate.getMillis() > 0) ? channel.setAdvertiseFrom(advertiseAvailableDate) : channel.setAdvertiseFrom(null);
                channelIdsThatHaveAvailableDateAdded.add(currentGuid);
            }
        }

        removeStaleAvailableDateFromChannel(channelIdsThatHaveAvailableDateAdded, channelResolver.all());
    }

    private void removeStaleAliasesFromChannel(Set<Long> channelIdsThatHaveAliasesAdded, Iterable<Channel> channels) {

        for(Channel channel : channels) {
            if(!channelIdsThatHaveAliasesAdded.contains(channel.getId())) {
                channel.setAliases(Collections.emptySet());
            }
        }
    }

    private void removeStaleAvailableDateFromChannel(Set<Long> channelIdsThatHaveAvailableDateAdded, Iterable<Channel> channels) {

        for(Channel channel : channels) {
            if(!channelIdsThatHaveAvailableDateAdded.contains(channel.getId())) {
                //channel.setAdvertiseFrom(null);
            }
        }
    }

}
