package org.atlasapi.remotesite.bt.channels;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClient;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClientException;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.query.Selection;


public class TargetUserGroupChannelGroupSaver extends AbstractBtChannelGroupSaver {
    
    private static final String TARGET_USER_GROUP = "targetUserGroup";
    private static final String TARGET_USER_GROUP_SCHEME = TARGET_USER_GROUP;
    private final String aliasUriPrefix;
    private final String aliasNamespace;
    private final BtMpxClient btMpxClient;
    private Map<String, String> labelToGuidMap;

    public TargetUserGroupChannelGroupSaver(Publisher publisher, String aliasUriPrefix, 
            String aliasNamespace, ChannelGroupResolver channelGroupResolver, 
            ChannelGroupWriter channelGroupWriter, BtMpxClient btMpxClient,
            ChannelResolver channelResolver, ChannelWriter channelWriter) {
        super(publisher, channelGroupResolver, channelGroupWriter, channelResolver, channelWriter,
                false, LoggerFactory.getLogger(TargetUserGroupChannelGroupSaver.class));
        
        this.aliasUriPrefix = checkNotNull(aliasUriPrefix);
        this.aliasNamespace = checkNotNull(aliasNamespace) + ":tug";
        this.btMpxClient = checkNotNull(btMpxClient);
    }
    
    @Override
    protected void start() {
        try {
            PaginatedEntries categories = btMpxClient.getCategories(Optional.<Selection>absent());
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            for (Entry category : categories.getEntries()) {
                if (TARGET_USER_GROUP.equals(category.getScheme())) {
                    builder.put(category.getTitle(), category.getGuid());
                }
            }
            labelToGuidMap = builder.build();
        } catch (BtMpxClientException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected List<String> keysFor(Entry channel) {
        ImmutableList.Builder<String> keys = ImmutableList.builder();
        for (Category category : channel.getCategories()) {
            if (TARGET_USER_GROUP_SCHEME.equals(category.getScheme())) {
                keys.add(fullTargetUserGroupFor(category.getName()));
            }  
        }
        return keys.build();
    }

    private String fullTargetUserGroupFor(String label) {
        String mapped = labelToGuidMap.get(label);
        checkState(mapped != null, "Could not map target user group label " + label);
        return mapped;
    }

    @Override
    protected Optional<Alias> aliasFor(String key) {
        return Optional.of(new Alias(aliasNamespace, key));
    }

    @Override
    protected String aliasUriFor(String key) {
        return aliasUriPrefix + key.replace("http://", "");
    }

    @Override
    protected String titleFor(String key) {
        return "BT Target User Group " + key;
    }
}
