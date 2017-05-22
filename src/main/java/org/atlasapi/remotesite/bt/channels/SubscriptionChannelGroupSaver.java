package org.atlasapi.remotesite.bt.channels;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


public class SubscriptionChannelGroupSaver extends AbstractBtChannelGroupSaver {

    private final String aliasUriPrefix;
    private final String aliasNamespace;

    public SubscriptionChannelGroupSaver(Publisher publisher, String aliasUriPrefix, 
            String aliasNamespace, ChannelGroupResolver channelGroupResolver, 
            ChannelGroupWriter channelGroupWriter, ChannelResolver channelResolver, 
            ChannelWriter channelWriter) {
        super(publisher, channelGroupResolver, channelGroupWriter, channelResolver, channelWriter,
                LoggerFactory.getLogger(SubscriptionChannelGroupSaver.class));
        
        this.aliasUriPrefix = checkNotNull(aliasUriPrefix);
        this.aliasNamespace = checkNotNull(aliasNamespace) + ":subscription-code";
    }
    
    @Override
    protected List<String> keysFor(Entry channel) {
        ImmutableList.Builder<String> keys = ImmutableList.builder();
        for (Category category : channel.getCategories()) {
            if ("subscription".equals(category.getScheme()))
                keys.add(category.getName());
        }
        return keys.build();
    }

    @Override
    protected Optional<Alias> aliasFor(String key) {
        return Optional.of(new Alias(aliasNamespace, key));
    }

    @Override
    protected String aliasUriFor(String key) {
        return aliasUriPrefix + key;
    }

    @Override
    protected String titleFor(String key) {
        return "BT subscription code " + key;
    }
}
