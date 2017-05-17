package org.atlasapi.equiv.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelType;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class ChannelMerger {

    private final Map<Long, Channel> dummyChannelStore;
    private final DateTime dateTime = DateTime.now();

    private ChannelMerger() {
        this.dummyChannelStore = Maps.newHashMap();

        Channel btProdIngestedChannel = Channel.builder()
                .withTitle("hello I am a sport channel")
                .withSource(Publisher.BT_TV_CHANNELS)
                .withAdvertiseFrom(dateTime.minusDays(1))
                .withAdvertiseTo(dateTime.plusDays(1))
                .build();
        btProdIngestedChannel.setId(111L);

        Channel btTest1IngestedChannel = Channel.builder()
                .withTitle("I am test ref channel")
                .withSource(Publisher.BT_TV_CHANNELS_TEST1)
                .withAdvertiseFrom(dateTime.minusDays(10))
                // omit advertiseTo to test equiv fallback
                .build();
        btTest1IngestedChannel.setId(888L);

        Channel existingChannel = Channel.builder()
                .withTitle("Existing Channel of Top Sport")
                .withChannelType(ChannelType.CHANNEL)
                .withSource(Publisher.METABROADCAST)
                .withAdvertiseFrom(dateTime.minusYears(1))
                .withAdvertiseTo(dateTime.plusYears(1))
                .withSameAs(ImmutableSet.of(btProdIngestedChannel.getId(), btTest1IngestedChannel.getId()))
                .build();
        existingChannel.setId(123L);

        Channel existingChannelWithNoFieldsSet = Channel.builder()
                .withTitle("Oh I wish I had my fields set")
                .withSource(Publisher.METABROADCAST)
                .withSameAs(ImmutableSet.of(btProdIngestedChannel.getId(), btTest1IngestedChannel.getId()))
                .build();
        existingChannelWithNoFieldsSet.setId(456L);

        dummyChannelStore.put(btProdIngestedChannel.getId(), btProdIngestedChannel);
        dummyChannelStore.put(existingChannel.getId(), existingChannel);
        dummyChannelStore.put(btTest1IngestedChannel.getId(), btTest1IngestedChannel);
        dummyChannelStore.put(existingChannelWithNoFieldsSet.getId(), existingChannelWithNoFieldsSet);
    }

    public static ChannelMerger create() {
        return new ChannelMerger();
    }

    public Channel mergeChannel(Application application, Channel channel) {

        if (!application.getConfiguration().isPrecedenceEnabled()) {
            return channel;
        }

        ImmutableList<Publisher> orderedPublishers = getOrderedPublishers(application);

        Map<Publisher, Channel> channelMap = Maps.newHashMap();

        channelMap.put(channel.getSource(), channel);
        channel.getSameAs().stream()
                .map(this::getChannel)
                .filter(channel1 -> application.getConfiguration()
                        .getEnabledReadSources()
                        .contains(channel1.getSource())
                )
                .forEach(channel1 -> channelMap.put(channel1.getSource(), channel1));

        Channel mergedChannel = Channel.builder(channel).build();
        mergedChannel.setId(channel.getId());

        mergeAdvertiseFrom(orderedPublishers, channelMap, mergedChannel);
        mergeAdvertiseTo(orderedPublishers, channelMap, mergedChannel);

        return mergedChannel;

    }

    private ImmutableList<Publisher> getOrderedPublishers(Application application) {
        Ordering<Publisher> ordering = application.getConfiguration()
                .getImageReadPrecedenceOrdering();

        return application.getConfiguration()
                .getEnabledReadSources()
                .stream()
                .sorted(ordering)
                .collect(MoreCollectors.toImmutableList());

    }

    private void mergeAdvertiseFrom(
            List<Publisher> publishers,
            Map<Publisher, Channel> channelMap,
            Channel mergedChannel
    ) {
        for (Publisher publisher : publishers) {
            Channel channel = channelMap.get(publisher);
            if(channel != null && channel.getAdvertiseFrom() != null) {
                mergedChannel.setAdvertiseFrom(channel.getAdvertiseFrom());
                break;
            }
        }

    }

    private void mergeAdvertiseTo(
            List<Publisher> publishers,
            Map<Publisher, Channel> channelMap,
            Channel mergedChannel
    ) {
        for (Publisher publisher : publishers) {
            Channel channel = channelMap.get(publisher);
            if(channel != null && channel.getAdvertiseTo() != null) {
                mergedChannel.setAdvertiseTo(channel.getAdvertiseTo());
                break;
            }
        }
    }

    public Channel getChannel(Long id) {
        return dummyChannelStore.get(id);
    }
}
