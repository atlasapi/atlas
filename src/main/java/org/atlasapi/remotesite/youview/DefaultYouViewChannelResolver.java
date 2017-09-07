package org.atlasapi.remotesite.youview;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    @Nullable
    public String getChannelUri(int serviceId) {
        Map<Integer, Channel> channelMap = getResolvedChannels().getChannelMap();

        if (channelMap.containsKey(serviceId)) {
            return channelMap.get(serviceId).getUri();
        }
        return null;
    }
    
    @Override
    public String getChannelServiceAlias(int serviceId) {
        return getResolvedChannels().getAliasMap().get(serviceId);
    }

    @Override
    public Optional<Channel> getChannel(int serviceId) {
        Map<Integer, Channel> channelMap = getResolvedChannels().getChannelMap();

        if (channelMap.containsKey(serviceId)) {
            return Optional.fromNullable(channelMap.get(serviceId));
        }
        return Optional.absent();
    }

    @Override
    public Iterable<Channel> getAllChannels() {
        return getResolvedChannels().getChannelMap().values();
    }

    @Override
    public Map<Integer, Channel> getAllChannelsByServiceId() {
        return getResolvedChannels().getChannelMap();
    }

    private ResolvedChannels getResolvedChannels() {
        return channelsCache.getUnchecked(CACHE_KEY);
    }

    private ResolvedChannels resolveChannels() {
        Map<Integer, Channel> channelMap = new LinkedHashMap<>();
        Map<Integer, String> aliasMap = new LinkedHashMap<>();

        for (String prefix : aliasPrefixes) {
            buildEntriesForPrefix(prefix, channelMap, aliasMap);
        }

        return ResolvedChannels.create(
                ImmutableMap.copyOf(channelMap),
                ImmutableMap.copyOf(aliasMap)
        );
    }

    private void buildEntriesForPrefix(
            String prefix,
            Map<Integer, Channel> channelMap,
            Map<Integer, String> aliasMap
    ) {
        Pattern pattern = Pattern.compile("^" + prefix + "(\\d+)$");
        Multimap<Channel, String> overrides = overrideAliasesForPrefix(channelResolver, prefix);
        Map<String, Channel> overridesByAlias = inverseMultimap(overrides);

        Set<Channel> foundMappings = Sets.newHashSet();
        for (Entry<String, Channel> entry : channelResolver.forAliases(prefix).entrySet()) {
            String channelAlias = entry.getKey();
            Channel channel = overridesByAlias.get(channelAlias.replace(
                    HTTP_PREFIX,
                    OVERRIDES_PREFIX
            ));

            String alias = channelAlias;
            if (channel == null) {
                channel = entry.getValue();
                alias = overrideFor(channel, overrides).or(channelAlias);
            }

            addService(pattern, alias, channel, channelMap, aliasMap);
            foundMappings.add(channel);
        }

        // ensure that where there's _only_ an override on a channel that's taken into account
        addOverridesWhereNoPrimaryAliasExists(
                pattern, foundMappings, overrides, channelMap, aliasMap
        );
    }

    private Map<String, Channel> inverseMultimap(Multimap<Channel, String> overrides) {
        ImmutableMap.Builder<String, Channel> overrideAliases = ImmutableMap.builder();

        for (Entry<Channel, String> entry : overrides.entries()) {
            overrideAliases.put(entry.getValue(), entry.getKey());
        }

        return overrideAliases.build();
    }

    private void addService(
            Pattern pattern,
            String alias,
            Channel channel,
            Map<Integer, Channel> channelMap,
            Map<Integer, String> aliasMap
    ) {
        Matcher m = pattern.matcher(alias);
        if (!m.matches()) {
            log.error("Could not parse YouView alias " + alias);
            return;
        }
        Integer serviceId = Integer.decode(m.group(1));

        checkAdd(channelMap, serviceId, channel);
        checkAdd(aliasMap, serviceId, alias);
    }

    private <T> void checkAdd(Map<Integer, T> map, Integer serviceId, T obj) {
        T old = map.put(serviceId, obj);
        if (old != null) {
            if (old.equals(obj)) {
                log.warn("{} added twice for serviceId={}: {}",
                        old.getClass().getSimpleName(), serviceId, old);
            } else {
                log.error("Multiple {}s with the same serviceId={}: {}, {}",
                        old.getClass().getSimpleName(), serviceId, old, obj);
            }
        }
    }

    private void addOverridesWhereNoPrimaryAliasExists(
            Pattern pattern,
            Set<Channel> foundMappings,
            Multimap<Channel, String> overrides,
            Map<Integer, Channel> channelMap,
            Map<Integer, String> aliasMap
    ) {
        for (Entry<Channel, String> override : overrides.entries()) {
            if (foundMappings.contains(override.getKey())) {
                continue;
            }
            addService(
                    pattern,
                    normaliseOverrideAlias(override.getValue()),
                    override.getKey(),
                    channelMap,
                    aliasMap
            );
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
        return Optional.absent();
    }

    private String normaliseOverrideAlias(String alias) {
        return alias.replace(OVERRIDES_PREFIX, HTTP_PREFIX);
    }

    private Multimap<Channel, String> overrideAliasesForPrefix(
            ChannelResolver channelResolver,
            String prefix
    ) {
        ImmutableMultimap.Builder<Channel, String> channelToAlias = ImmutableMultimap.builder();
        Map<String, Channel> channelMap = channelResolver
                .forAliases(prefix.replace(HTTP_PREFIX, OVERRIDES_PREFIX));

        for (Entry<String, Channel> entry:  channelMap.entrySet()) {
            channelToAlias.put(entry.getValue(), entry.getKey());
        }
        return channelToAlias.build();
    }

    private static class ResolvedChannels {

        private final Map<Integer, Channel> channelMap;
        private final Map<Integer, String> aliasMap;

        private ResolvedChannels(
                Map<Integer, Channel> channelMap,
                Map<Integer, String> aliasMap
        ) {
            this.channelMap = checkNotNull(channelMap);
            this.aliasMap = checkNotNull(aliasMap);
        }

        public static ResolvedChannels create(
                Map<Integer, Channel> channelMap,
                Map<Integer, String> aliasMap
        ) {
            return new ResolvedChannels(channelMap, aliasMap);
        }

        public Map<Integer, Channel> getChannelMap() {
            return channelMap;
        }

        public Map<Integer, String> getAliasMap() {
            return aliasMap;
        }
    }
}
