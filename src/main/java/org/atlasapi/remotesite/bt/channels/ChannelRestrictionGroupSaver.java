package org.atlasapi.remotesite.bt.channels;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelRestrictionGroupSaver extends AbstractBtChannelGroupSaver {

    private static final String IS_CASTABLE_RAW = "isCastable:true";
    private static final String IS_CASTABLE_TITLE = "BT channels available via Chromecast";
    private static final String IS_CASTABLE_URL = "chromecastable";

    private static final String LARGE_SCREEN_RAW = "largeScreenScode:S0360435";
    private static final String LARGE_SCREEN_TITLE = "BT channels available via the BT TV app on a large screen";
    private static final String LARGE_SCREEN_URL = "large-screen-viewable";

    private static final Map<String, IdPair> RESTRICTION_MAP = ImmutableMap.<String, IdPair>builder()
            .put(IS_CASTABLE_RAW, new IdPair(IS_CASTABLE_TITLE, IS_CASTABLE_URL))
            .put(LARGE_SCREEN_RAW, new IdPair(LARGE_SCREEN_TITLE, LARGE_SCREEN_URL))
            .build();

    private final String aliasUriPrefix;
    private final String aliasNamespace;

    public ChannelRestrictionGroupSaver(
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
                LoggerFactory.getLogger(ChannelRestrictionGroupSaver.class)
        );

        this.aliasUriPrefix = checkNotNull(aliasUriPrefix);
        this.aliasNamespace = checkNotNull(aliasNamespace) + ":channel-restriction";
    }

    @Override
    protected List<String> keysFor(Entry channel) {

        return channel.getCategories().stream()
                .filter(category -> "channelRestriction".equals(category.getScheme()))
                .map(Category::getName)
                .filter(RESTRICTION_MAP::containsKey)
                .collect(MoreCollectors.toImmutableList());
    }

    @Override
    protected Optional<Alias> aliasFor(String key) {
        return Optional.of(new Alias(aliasNamespace, key));
    }

    @Override
    protected String aliasUriFor(String key) {
        return aliasUriPrefix + RESTRICTION_MAP.get(key).getUrlSuffix();
    }

    @Override
    protected String titleFor(String key) {
            return RESTRICTION_MAP.get(key).getTitle();
        }

    private static class IdPair {
        private final String title;
        private final String urlSuffix;

        public IdPair(String title, String urlSuffix) {
            this.title = checkNotNull(title);
            this.urlSuffix = checkNotNull(urlSuffix);
        }

        public String getTitle() {
            return title;
        }

        public String getUrlSuffix() {
            return urlSuffix;
        }

    }

}
