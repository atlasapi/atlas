package org.atlasapi.input;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

public class ClipModelTransformer extends ItemModelTransformer  {
    public ClipModelTransformer(LookupEntryStore lookupStore, TopicStore topicStore, 
            ChannelResolver channelResolver, NumberToShortStringCodec idCodec, Clock clock, SegmentModelTransformer segmentModelTransformer) {
        super(lookupStore, topicStore, channelResolver, idCodec, null, clock, segmentModelTransformer);
    }

    @Override
    protected Item createOutput(org.atlasapi.media.entity.simple.Item inputItem) {
        return new Clip();
    }

    @Override
    protected Item setFields(Item result, org.atlasapi.media.entity.simple.Item inputItem) {
        super.setFields(result, inputItem);
        DateTime now = this.clock.now();
        result.setLastUpdated(now);
        return result;
    }
}
