package org.atlasapi.remotesite.bt.channels;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelRestrictionGroupSaver extends AbstractBtChannelGroupSaver {

    private static final String IS_CASTABLE_RAW = "isCastable:true";
    private static final String IS_CASTABLE_TITLE = "BT channels available via Chromecast";
    private static final String IS_CASTABLE_URL = "chromecastable";

    private static final String LARGE_SCREEN_RAW = "largeScreenScode";
    private static final String LARGE_SCREEN_TITLE = "BT channels available via the BT TV app on a large screen";
    private static final String LARGE_SCREEN_URL = "large-screen-viewable";

    private static final Map<String, Restriction> RESTRICTION_MAP = Maps.uniqueIndex(
            ImmutableSet.of(
                    new Restriction(IS_CASTABLE_RAW, IS_CASTABLE_TITLE, IS_CASTABLE_URL),
                    new Restriction(LARGE_SCREEN_RAW, LARGE_SCREEN_TITLE, LARGE_SCREEN_URL)
            ),
            Restriction::getKey
    );

    private final String aliasUriPrefix;
    private final String aliasNamespace;
    private final ConcurrentMap<String, Set<Alias>> restrictionAliases = new ConcurrentHashMap<>();

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

    @Override protected void start() {
        restrictionAliases.clear();
    }

    @Nullable private static String withoutSuffix(String s) {
        int i = s.lastIndexOf(':');
        if (i >= 0) return s.substring(0, i);
        return null;
    }

    @Nullable private Restriction getRestriction(@Nullable String key) {
        if (key == null) return null;
        String pre = key;
        do {
            Restriction restriction = RESTRICTION_MAP.get(pre);
            if (restriction != null) {
                return restriction;
            }
            pre = withoutSuffix(pre);
        } while (pre != null);
        return null;
    }

    private Restriction getRestrictionChecked(String key)
            throws IllegalArgumentException {                                           // NOSONAR
        Restriction restriction = getRestriction(key);
        if (restriction != null) return restriction;
        throw new IllegalArgumentException("Key is not valid for a restriction: " + key);
    }

    private @Nullable String cacheAliasGetKey(@Nullable String name) {
        Restriction restriction = getRestriction(name);
        if (restriction == null) return null;
        Set<Alias> aliases = restrictionAliases.computeIfAbsent(
                restriction.getKey(),
                k -> Collections.newSetFromMap(new ConcurrentHashMap<>())
        );
        aliases.add(new Alias(aliasNamespace, name));
        return restriction.getKey();
    }

    @Override
    protected List<String> keysFor(Entry channel) {
        return channel.getCategories().stream()
                .filter(category -> "channelRestriction".equals(category.getScheme()))
                .map(Category::getName)
                .map(this::cacheAliasGetKey)
                .filter(Objects::nonNull)
                .distinct()
                .collect(MoreCollectors.toImmutableList());
    }

    @Override
    protected Set<Alias> aliasesFor(String key) {
        Set<Alias> aliases = restrictionAliases.get(getRestrictionChecked(key).getKey());
        return Collections.unmodifiableSet(aliases);    // don't take a copy until we have to
    }

    @Override
    protected String aliasUriFor(String key) {
        return aliasUriPrefix + getRestrictionChecked(key).getUrlSuffix();
    }

    @Override
    protected String titleFor(String key) {
        return getRestrictionChecked(key).getTitle();
    }

    private static class Restriction {
        private final String key;
        private final String title;
        private final String urlSuffix;

        public Restriction(String key, String title, String urlSuffix) {
            this.key = checkNotNull(key);
            this.title = checkNotNull(title);
            this.urlSuffix = checkNotNull(urlSuffix);
        }

        public String getKey() {
            return key;
        }

        public String getTitle() {
            return title;
        }

        public String getUrlSuffix() {
            return urlSuffix;
        }

    }

}
