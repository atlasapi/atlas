package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.util.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.sun.istack.NotNull;
import org.atlasapi.media.channel.Channel;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

public class BtChannelDataUpdater {

    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec();

    public void addAliasesAndAvailableDateToChannel(PaginatedEntries paginatedEntries, ChannelResolver channelResolver) {

        List<Entry> entries = paginatedEntries.getEntries();

        Set<Long> channelIdsThatHaveAliasesAndAvailableDateAdded = Sets.newHashSet();

        for(int i = 0; i < entries.size(); i++) {
            Entry currentEntry = entries.get(i);
            String linearEpgChannelId = currentEntry.getLinearEpgChannelId();
            DateTime advertiseAvailableDate = currentEntry.getAvailableDate();
            long currentGuid = codec.decode(currentEntry.getGuid()).longValue();

            Maybe<Channel> channelMaybe = channelResolver.fromId(currentGuid);

            if(channelMaybe.hasValue()) {
                Channel channel = channelMaybe.requireValue();
                Set<Alias> aliases = channel.getAliases();

                aliases.removeIf(alias -> {

                    if (alias.getNamespace().equals(linearEpgChannelId)) {
                        return true;
                    }
                    return false;
                });

                //Using "namespace" as namespace for now.
                channel.addAlias(new Alias("namespace", linearEpgChannelId));

                //Turned off for now until it is added to atlas model and atlas deer. Did a git pull but couldn't get dias' changes probably because it is only on stage. (my branch is based off master)
                //(advertiseAvailableDate != null && advertiseAvailableDate.getMillis() > 0) ? channel.setAdvertiseFrom(advertiseAvailableDate) : channel.setAdvertiseFrom(null);
                channelIdsThatHaveAliasesAndAvailableDateAdded.add(currentGuid);
            }
        }

        removeStaleAliasesAndAvailableDateFromChannel(channelIdsThatHaveAliasesAndAvailableDateAdded, channelResolver.all());
    }

    private void removeStaleAliasesAndAvailableDateFromChannel(Set<Long> channelIdsThatHaveAliasesAndAvailableDateAdded, Iterable<Channel> channels) {
        Iterable<Channel> channelsThatHasStaleAliasesAndAvailableDates = Iterables.filter(channels,
                                                                            shouldRemoveValuesFromChannel(channelIdsThatHaveAliasesAndAvailableDateAdded));

        StreamSupport.stream(channelsThatHasStaleAliasesAndAvailableDates.spliterator(), false).forEach(channel -> {
            channel.setAliases(Collections.emptySet());
            //channel.setAdvertiseForm(null);
        });
    }

    private Predicate<Channel> shouldRemoveValuesFromChannel(final Set<Long> channelIdsThatHaveAliasesAndAvailableDateAdded) {
        return new Predicate<Channel>() {
            @Override
            public boolean apply(@NotNull Channel channel) {
                return !channelIdsThatHaveAliasesAndAvailableDateAdded.contains(channel.getId());
            }
        };
    }
}
