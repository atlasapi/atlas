package org.atlasapi.remotesite.youview;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class DefaultYouViewChannelResolver implements YouViewChannelResolver {
    
    private static final String HTTP_PREFIX = "http://";

    private static final String OVERRIDES_PREFIX = "http://overrides.";

    private static final Logger log = LoggerFactory.getLogger(DefaultYouViewChannelResolver.class);
    
    private final Map<Integer, Channel> channelMap;
    private final Map<Integer, String> aliasMap;
    
    public DefaultYouViewChannelResolver(ChannelResolver channelResolver, Set<String> aliasPrefixes) {
        Builder<Integer, Channel> channelMapBuilder = ImmutableMap.builder();
        Builder<Integer, String> aliasMapBuilder = ImmutableMap.builder();
        
        for (String prefix : aliasPrefixes) {
            buildEntriesForPrefix(channelResolver, prefix, channelMapBuilder, aliasMapBuilder);
        }
        
        channelMap = channelMapBuilder.build();
        aliasMap = aliasMapBuilder.build();
    }
    
    private void buildEntriesForPrefix(ChannelResolver channelResolver, 
            String prefix, Builder<Integer, Channel> channelMapBuilder,
            Builder<Integer, String> aliasMapBuilder) {
        
        Pattern pattern = Pattern.compile("^" + prefix + "(\\d+)$");
        Multimap<Channel, String> overrides = overrideAliasesForPrefix(channelResolver, prefix);
        Set<Channel> foundMappings = Sets.newHashSet();
        for (Entry<String, Channel> entry : channelResolver.forAliases(prefix).entrySet()) {
            Channel channel = entry.getValue();
            String alias = overrideFor(channel, overrides).or(entry.getKey());
            addService(pattern, alias, channel, channelMapBuilder, aliasMapBuilder);
            foundMappings.add(channel);
        }
        
        // ensure that where there's _only_ an override on a channel, that's 
        // taken into account
        
        addOverridesWhereNoPrimaryAliasExists(pattern, foundMappings, overrides, channelMapBuilder, aliasMapBuilder);
    }

    private void addService(Pattern pattern, String alias, Channel channel,
            Builder<Integer, Channel> channelMapBuilder, Builder<Integer, String> aliasMapBuilder) {
        Matcher m = pattern.matcher(alias);
        if (!m.matches()) {
            log.error("Could not parse YouView alias " + alias);
            return;
        }
        Integer serviceId = Integer.decode(m.group(1));
        
        channelMapBuilder.put(serviceId, channel);
        aliasMapBuilder.put(serviceId, alias);
    }
    
    private void addOverridesWhereNoPrimaryAliasExists(Pattern pattern, Set<Channel> foundMappings,
            Multimap<Channel, String> overrides, Builder<Integer, Channel> channelMapBuilder, 
            Builder<Integer, String> aliasMapBuilder) {
        
        for (Entry<Channel, String> override : overrides.entries()) {
            if (foundMappings.contains(override.getKey())) {
                continue;
            }
            addService(pattern, normaliseOverrideAlias(override.getValue()), override.getKey(), 
                    channelMapBuilder, aliasMapBuilder);
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
                log.warn("Multiple override aliases found on single channel, taking first " + overrideAliases);
            }
            return Optional.of(normaliseOverrideAlias(overrideAliases.iterator().next()));
        }
        return Optional.absent();
    }
    
    private String normaliseOverrideAlias(String alias) {
        return alias.replace(OVERRIDES_PREFIX, HTTP_PREFIX);
    }
    
    private Multimap<Channel, String> overrideAliasesForPrefix(ChannelResolver channelResolver, String prefix) {
        ImmutableMultimap.Builder<Channel, String> channelToAlias = ImmutableMultimap.builder();
        for (Entry<String, Channel> entry: channelResolver.forAliases(prefix.replace(HTTP_PREFIX, OVERRIDES_PREFIX)).entrySet()) {
            channelToAlias.put(entry.getValue(), entry.getKey());
        }
        return channelToAlias.build();
    }

    @Override
    public String getChannelUri(int serviceId) {
        if (channelMap.containsKey(serviceId)) {
            return channelMap.get(serviceId).getUri();
        }
        return null;
    }
    
    @Override
    public String getChannelServiceAlias(int serviceId) {
        return aliasMap.get(serviceId);
    }
    
    @Override
    public Optional<Channel> getChannel(int serviceId) {
        if (channelMap.containsKey(serviceId)) {
            return Optional.fromNullable(channelMap.get(serviceId));
        }
        return Optional.absent();
    }

    @Override
    public Iterable<Channel> getAllChannels() {
        return channelMap.values();
    }

    @Override
    public Map<Integer, Channel> getAllChannelsByServiceId() {
        return channelMap;
    }

}
