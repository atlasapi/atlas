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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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

    private boolean hasRestriction(@Nullable String key) {
        return getRestriction(key) != null;
    }

    private Restriction getRestrictionChecked(String key)
            throws IllegalArgumentException {                                           // NOSONAR
        Restriction restriction = getRestriction(key);
        if (restriction != null) return restriction;
        throw new IllegalArgumentException("Key is not valid for a restriction: " + key);
    }

    @Override
    protected List<String> keysFor(Entry channel) {
        return channel.getCategories().stream()
                .filter(category -> "channelRestriction".equals(category.getScheme()))
                .map(Category::getName)
                .filter(this::hasRestriction)
                .collect(MoreCollectors.toImmutableSet()).asList();
    }

    @Override
    protected Set<Alias> aliasesFor(String key) {
        getRestrictionChecked(key);
        return ImmutableSet.of(new Alias(aliasNamespace, key));
    }

    @Override
    protected String aliasUriFor(String key) {
        return aliasUriPrefix + getRestrictionChecked(key).getUrlSuffix(key);
    }

    @Override
    protected String titleFor(String key) {
        return getRestrictionChecked(key).getTitle(key);
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

        public String getTitle(String key) {
            if (this.key.equals(key)) {
                return urlSuffix;
            }
            return title + " (" + getSuffix(key) + ")";
        }

        private static final Pattern INVALID_CHARS = Pattern.compile("[^\\p{Alnum}]+");
        public String getUrlSuffix(String key) {
            if (this.key.equals(key)) {
                return urlSuffix;
            }
            String suffix = INVALID_CHARS.matcher(getSuffix(key).toLowerCase()).replaceAll("-");
            return urlSuffix + "-" + suffix;
        }

        private String getSuffix(String key) {
            if (!(key.startsWith(this.key) && key.charAt(this.key.length()) == ':')) {
                throw new IllegalArgumentException(
                        "Invalid key: " + key + " for restriction key: " + this.key
                );
            }
            return key.substring(this.key.length() + 1);
        }
    }

}
