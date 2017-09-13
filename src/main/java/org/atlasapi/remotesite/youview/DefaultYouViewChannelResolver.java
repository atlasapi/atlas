package org.atlasapi.remotesite.youview;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultYouViewChannelResolver implements YouViewChannelResolver {

    private static final Logger log = LoggerFactory.getLogger(DefaultYouViewChannelResolver.class);

    private static final String CACHE_KEY = "key";

    private static final String HTTP_PREFIX = "http://";
    private static final String OVERRIDES_PREFIX = "http://overrides.";

    private final ChannelResolver channelResolver;
    private final Set<String> aliasPrefixes;

    // The channels have to be resolved in their entirety every time so that overrides can be
    // processed correctly. To cope with that this cache resolves all channels at once and stores
    // the result under a single key
    private final LoadingCache<String, ResolvedChannels> channelsCache = CacheBuilder
            .newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build(
                    new CacheLoader<String, ResolvedChannels>() {

                        @Override
                        public ResolvedChannels load(String key) throws Exception {
                            return resolveChannels();
                        }
                    }
            );

    private DefaultYouViewChannelResolver(
            ChannelResolver channelResolver,
            Set<String> aliasPrefixes
    ) {
        this.channelResolver = checkNotNull(channelResolver);
        this.aliasPrefixes = checkNotNull(aliasPrefixes);
    }

    public static DefaultYouViewChannelResolver create(
            ChannelResolver channelResolver,
            Set<String> aliasPrefixes
    ) {
       return new DefaultYouViewChannelResolver(channelResolver, aliasPrefixes);
    }

    @Override
    public Collection<String> getChannelUris(int serviceId) {
        return Collections2.transform(
                getChannels(serviceId),
                Channel::getUri
        );
    }
    
    @Override
    public Collection<String> getServiceAliases(Channel channel) {
        return getResolvedChannels().getAliases(channel);
    }

    @Override
    public Collection<Channel> getChannels(int serviceId) {
        return getResolvedChannels().getChannels(serviceId);
    }

    @Override
    public Collection<Channel> getAllChannels() {
        return getResolvedChannels().getAllChannels();
    }

    @Override
    public Multimap<Integer, Channel> getAllServiceIdsToChannels() {
        return getResolvedChannels().getAllServiceIdsToChannels();
    }

    @Override
    public Collection<Integer> getServiceIds(Channel channel) {
        return getResolvedChannels().getServiceIds(channel);
    }

    private ResolvedChannels getResolvedChannels() {
        return channelsCache.getUnchecked(CACHE_KEY);
    }

    private ResolvedChannels resolveChannels() {
        ResolvedChannels.Builder resolvedChannels = ResolvedChannels.builder();
        for (String prefix : aliasPrefixes) {
            buildEntriesForPrefix(prefix, resolvedChannels);
        }
        return resolvedChannels.build();
    }

    private void buildEntriesForPrefix(
            String prefix,
            ResolvedChannels.Builder resolvedChannels
    ) {
        // match the alias followed by the serviceId as the first group, e.g.:
        // ^http://youview.com/services/(12345)$
        Pattern pattern = Pattern.compile("^" + prefix + "(\\d+)$");
        ImmutableMultimap<Channel, String> overrides = overrideAliasesForPrefix(prefix);

        for (Entry<String, Channel> entry : channelResolver.allForAliases(prefix).entries()) {
            String channelAlias = entry.getKey();
            Channel channel = Iterables.getFirst(
                    overrides.inverse().get(
                            channelAlias.replace(HTTP_PREFIX, OVERRIDES_PREFIX)
                    ),
                    null
            );

            String alias = channelAlias;
            if (channel == null) {
                channel = entry.getValue();
                alias = overrideFor(channel, overrides).orElse(channelAlias);
            }

            boolean added = resolvedChannels.addService(
                    extractServiceId(pattern, alias),
                    alias,
                    channel
            );
            if (!added) {
                log.warn("Service not added: serviceId={} alias={} channel={}",
                        extractServiceId(pattern, alias),
                        alias,
                        channel);
            }
        }

        // ensure that where there's _only_ an override on a channel that's taken into account
        addOverridesWhereNoPrimaryAliasExists(
                pattern, overrides, resolvedChannels
        );
    }

    @Nullable
    private Integer extractServiceId(Pattern pattern, String alias) {
        Matcher m = pattern.matcher(alias);
        if (m.matches()) {
            return Integer.decode(m.group(1));
        } else {
            log.error("Could not parse YouView alias " + alias);
            return null;
        }
    }

    private void addOverridesWhereNoPrimaryAliasExists(
            Pattern pattern,
            Multimap<Channel, String> overrides,
            ResolvedChannels.Builder resolvedChannels
    ) {
        for (Entry<Channel, String> override : overrides.entries()) {
            if (resolvedChannels.contains(override.getKey())) {
                continue;
            }
            String normalisedAlias = normaliseOverrideAlias(override.getValue());
            boolean added = resolvedChannels.addService(
                    extractServiceId(pattern, normalisedAlias),
                    normalisedAlias,
                    override.getKey()
            );
            if (!added) {
                log.warn("Service not added: serviceId={} alias={} channel={}",
                        extractServiceId(pattern, normalisedAlias),
                        normalisedAlias,
                        override.getKey());
            }
        }
    }

    /**
     * Provide the override mapping for a channel, if it exists, having rewritten it to
     * use the standard, non-override, URI. For example http://override.youview.com/service/1
     * will be rewritten as http://youview.com/service/1
     */
    private Optional<String> overrideFor(Channel channel, Multimap<Channel, String> overrides) {
        Collection<String> overrideAliases = overrides.get(channel);
        if (!overrideAliases.isEmpty()) {
            if (overrideAliases.size() > 1) {
                log.warn("Multiple override aliases found on single channel, taking first " +
                        overrideAliases);
            }
            return Optional.of(normaliseOverrideAlias(overrideAliases.iterator().next()));
        }
        return Optional.empty();
    }

    private String normaliseOverrideAlias(String alias) {
        return alias.replace(OVERRIDES_PREFIX, HTTP_PREFIX);
    }

    private ImmutableMultimap<Channel, String> overrideAliasesForPrefix(String prefix) {
        Multimap<String, Channel> channelMap = channelResolver
                .allForAliases(prefix.replace(HTTP_PREFIX, OVERRIDES_PREFIX));

        return ImmutableMultimap.copyOf(channelMap).inverse();
    }

    /* So something has gone weird with YouView serviceIDs/aliases recently,
     * so the mapping can be something like:
     * <pre>
     *                                                      Channel (abc1)
     *                                              ,-----'
     *                       Alias (bt.yv.com/12345)
     *                ,-----'                       `-----.
     *   S ID (12345)                                       Channel (abc2)
     *                `-----.                     ,-------'
     *                       Alias (yv.com/12345)
     *                                            `-------.
     *                                                      Channel (abc3)
     *                                              ,-----'
     *                       Alias (yv.com/67890)
     *                ,-----'                       `-----.
     *   S ID (67890)                                       Channel (abc2)
     * </pre>
     */
    private static class ResolvedChannels {

        private final ImmutableSetMultimap<Integer, Channel> serviceId2channel;
        private final ImmutableSetMultimap<Channel, String> channel2alias;
        private final ImmutableSetMultimap<String, Integer> alias2serviceId;

        private ResolvedChannels(Builder builder) {
            this.serviceId2channel = ImmutableSetMultimap.copyOf(builder.serviceId2channel);
            this.channel2alias = ImmutableSetMultimap.copyOf(builder.channel2alias);
            this.alias2serviceId = ImmutableSetMultimap.copyOf(builder.alias2serviceId);
        }

        public boolean contains(Channel channel) {
            return channel2alias.containsKey(channel);
        }
        public boolean contains(Integer serviceId) {
            return serviceId2channel.containsKey(serviceId);
        }
        public boolean contains(String alias) {
            return alias2serviceId.containsKey(alias);
        }

        public Set<Channel> getAllChannels() {
            return channel2alias.keySet();
        }

        public Multimap<Integer, Channel> getAllServiceIdsToChannels() {
            return serviceId2channel;
        }

        public Set<Channel> getChannels(int serviceId) {
            return serviceId2channel.get(serviceId);
        }

        public Set<String> getAliases(Channel channel) {
            return channel2alias.get(channel);
        }

        public Set<Integer> getServiceIds(Channel channel) {
            return serviceId2channel.inverse().get(channel);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private Multimap<Integer, Channel> serviceId2channel;
            private Multimap<Channel, String> channel2alias;
            private Multimap<String, Integer> alias2serviceId;

            public Builder() {
                serviceId2channel = LinkedHashMultimap.create();
                channel2alias = LinkedHashMultimap.create();
                alias2serviceId = LinkedHashMultimap.create();
            }

            public boolean addService(
                    @Nullable Integer serviceId,
                    @Nullable String alias,
                    @Nullable Channel channel
            ) {
                if (serviceId == null || alias == null || channel == null) {
                    return false;
                }
                boolean added = true;
                added &= serviceId2channel.put(serviceId, channel);
                added &= channel2alias.put(channel, alias);
                added &= alias2serviceId.put(alias, serviceId);
                return added;
            }

            public boolean contains(Channel channel) {
                return channel2alias.containsKey(channel);
            }
            public boolean contains(Integer serviceId) {
                return serviceId2channel.containsKey(serviceId);
            }
            public boolean contains(String alias) {
                return alias2serviceId.containsKey(alias);
            }

            public ResolvedChannels build() {
                return new ResolvedChannels(this);
            }
        }
    }
}
