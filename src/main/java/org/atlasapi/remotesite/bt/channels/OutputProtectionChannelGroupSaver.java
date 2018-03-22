package org.atlasapi.remotesite.bt.channels;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;


public class OutputProtectionChannelGroupSaver extends AbstractBtChannelGroupSaver {

    private final String aliasUriPrefix;

    public OutputProtectionChannelGroupSaver(
            Publisher publisher,
            String aliasUriPrefix,
            String aliasNamespace,
            ChannelGroupResolver channelGroupResolver,
            ChannelGroupWriter channelGroupWriter,
            ChannelResolver channelResolver,
            ChannelWriter channelWriter
    ) {
        super(
                publisher,
                channelGroupResolver,
                channelGroupWriter,
                channelResolver,
                channelWriter,
                LoggerFactory.getLogger(OutputProtectionChannelGroupSaver.class)
        );
        
        this.aliasUriPrefix = checkNotNull(aliasUriPrefix);
    }

    @Override
    protected List<String> keysFor(Entry channel) {
        if (channel.hasOutputProtection()) {
            return ImmutableList.of("1");
        }
        return ImmutableList.of();
    }

    @Override
    protected Set<Alias> aliasesFor(String key) {
        return ImmutableSet.of();
    }

    @Override
    protected String aliasUriFor(String key) {
        return aliasUriPrefix + "outputprotection";
    }

    @Override
    protected String titleFor(String key) {
        return "BT channels with output protection";
    }

}
