package org.atlasapi.remotesite.youview;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
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
import java.util.stream.Collectors;

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
    public Collection<Channel> getChannels(int serviceId) {
        return getResolvedChannels().getChannels(serviceId);
    }

    @Override
    public Collection<Channel> getAllChannels() {
        return getResolvedChannels().getAllChannels();
    }

    @Override
    public Multimap<ServiceId, Channel> getAllServiceIdsToChannels() {
        return getResolvedChannels().getAllServiceIdsToChannels();
    }

    @Override
    public Collection<ServiceId> getServiceIds(Channel channel) {
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

            boolean added;
            try {
                added = resolvedChannels.addService(
                        ImmutableServiceId.create(prefix, alias),
                        channel
                );
            } catch (NullPointerException | IllegalArgumentException e) {
                added = false;
            }
            if (!added) {
                log.warn("Service not added: prefix={} alias={} channel={}",
                        prefix, alias, channel);
            }
        }

        // ensure that where there's _only_ an override on a channel that's taken into account
        addOverridesWhereNoPrimaryAliasExists(prefix, overrides, resolvedChannels);
    }

    private void addOverridesWhereNoPrimaryAliasExists(
            String prefix,
            Multimap<Channel, String> overrides,
            ResolvedChannels.Builder resolvedChannels
    ) {
        for (Entry<Channel, String> override : overrides.entries()) {
            if (resolvedChannels.contains(override.getKey())) {
                continue;
            }
            String normalisedAlias = normaliseOverrideAlias(override.getValue());
            boolean added;
            try {
                added = resolvedChannels.addService(
                        ImmutableServiceId.create(prefix, normalisedAlias),
                        override.getKey()
                );
            } catch (NullPointerException | IllegalArgumentException e) {
                added = false;
            }
            if (!added) {
                log.warn("Service not added: prefix={} alias={} channel={}",
                        prefix, normalisedAlias, override.getKey());
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
                log.warn("Multiple override aliases found on single channel, taking first {}",
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

        private final ImmutableSetMultimap<ServiceId, Channel> channelMap;
        private final ImmutableSetMultimap<Integer, ServiceId> serviceIdMap;
        private final ImmutableSetMultimap<String, ServiceId> aliasMap;

        private ResolvedChannels(Builder builder) {
            this.channelMap = ImmutableSetMultimap.copyOf(builder.channelMap).inverse();
            this.serviceIdMap = ImmutableSetMultimap.copyOf(builder.serviceIdMap);
            this.aliasMap = ImmutableSetMultimap.copyOf(builder.aliasMap);
        }

        public boolean contains(Channel channel) {
            return channelMap.inverse().containsKey(channel);
        }
        public boolean contains(int serviceId) {
            return serviceIdMap.containsKey(serviceId);
        }
        public boolean contains(String alias) {
            return aliasMap.containsKey(alias);
        }

        public ImmutableSet<Channel> getAllChannels() {
            return channelMap.inverse().keySet();
        }

        public ImmutableMultimap<ServiceId, Channel> getAllServiceIdsToChannels() {
            return channelMap;
        }

        public ImmutableSet<Channel> getChannels(int serviceId) {
            // Ideally this would return a view, but there's nothing in the standard libs/guava,
            // so we'll just have to accept the memory churn.
            // Note also, that we can't just use MoreCollectors.toImmutableSet as there may be
            // duplicates, which would cause the ImmutableSet builder to throw, so instead collect
            // into an ordinary set, and return an immutable copy.
            return ImmutableSet.copyOf(
                    serviceIdMap.get(serviceId).stream()
                            .map(channelMap::get)
                            .flatMap(Set::stream)
                            .collect(Collectors.toSet())
            );
        }

        public ImmutableSet<ServiceId> getServiceIds(Channel channel) {
            return channelMap.inverse().get(channel);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            // This is reversed so contains(channel) is efficient.
            private Multimap<Channel, ServiceId> channelMap;
            private Multimap<Integer, ServiceId> serviceIdMap;
            private Multimap<String, ServiceId> aliasMap;

            public Builder() {
                channelMap = LinkedHashMultimap.create();
                serviceIdMap = LinkedHashMultimap.create();
                aliasMap = LinkedHashMultimap.create();
            }

            public boolean addService(ImmutableServiceId serviceId, Channel channel)
                    throws NullPointerException     //NOSONAR RedundantThrows
            {
                //noinspection ConstantConditions   - need to be sure
                if (serviceId == null) throw new NullPointerException("serviceId is null");
                //noinspection ConstantConditions   - need to be sure
                if (channel == null) throw new NullPointerException("channel is null");

                boolean added = channelMap.put(channel, serviceId);
                if (!added) return false;   // already been added
                serviceIdMap.put(serviceId.getId(), serviceId);
                aliasMap.put(serviceId.getAlias(), serviceId);
                return true;
            }

            public boolean contains(@Nullable Channel channel) {
                return channel != null && channelMap.containsKey(channel);
            }
            public boolean contains(@Nullable ServiceId serviceId) {
                return serviceId != null && serviceIdMap.get(serviceId.getId()).contains(serviceId);
            }
            public boolean contains(@Nullable Integer serviceId) {
                return serviceId != null && serviceIdMap.containsKey(serviceId);
            }
            public boolean contains(@Nullable String alias) {
                return alias != null && aliasMap.containsKey(alias);
            }

            public ResolvedChannels build() {
                return new ResolvedChannels(this);
            }
        }

    }

    public static final class ImmutableServiceId extends ServiceId {
        private final int id;
        private final String prefix;
        private final String alias;
        private ImmutableServiceId(int id, String prefix, String alias) {
            this.id = id;
            this.prefix = checkNotNull(prefix);
            this.alias = checkNotNull(alias);
        }
        public static ImmutableServiceId create(String prefix, String alias)
                throws NullPointerException, IllegalArgumentException   //NOSONAR RedundantThrows
        {
            //noinspection ConstantConditions   - need to be sure
            if (prefix == null) throw new NullPointerException("prefix is null");
            //noinspection ConstantConditions   - need to be sure
            if (alias == null) throw new NullPointerException("alias is null");
            try {
                return new ImmutableServiceId(
                        Integer.parseInt(alias.substring(prefix.length())),
                        prefix,
                        alias
                );
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid ID in alias: " + alias);
            }
        }

        @Override public int getId() {
            return id;
        }

        @Override public String getPrefix() {
            return prefix;
        }

        @Override public String getAlias() {
            return alias;
        }

        private transient String str;   //NOSONAR transient - cached value
        @Override public String toString() {
            return (str == null) ? (str = super.toString()) : str;
        }
    }
}
