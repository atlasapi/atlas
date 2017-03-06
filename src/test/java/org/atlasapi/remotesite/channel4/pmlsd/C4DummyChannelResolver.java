package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;


public class C4DummyChannelResolver implements ChannelResolver {

    private final Channel FM = new Channel(Publisher.METABROADCAST, "4Music", "4music", false, MediaType.VIDEO, "http://www.4music.com");
    private final Channel E4 = new Channel(Publisher.METABROADCAST, "E4", "e4", false, MediaType.VIDEO, "http://www.e4.com");
    private final Channel F4 = new Channel(Publisher.METABROADCAST, "Film4", "film4", false, MediaType.VIDEO, "http://film4.com");
    private final Channel C4 = new Channel(Publisher.METABROADCAST, "Channel 4", "channel4", false, MediaType.VIDEO, "http://www.channel4.com");
    private final Channel M4 = new Channel(Publisher.METABROADCAST, "More4", "more4", false, MediaType.VIDEO, "http://www.channel4.com/more4");
    private final Channel FS = new Channel(Publisher.METABROADCAST, "4seven", "4seven", false, MediaType.VIDEO, "http://www.channel4.com/4seven");

    private ImmutableSet<Channel> channels = ImmutableSet.of(FM, E4, F4, C4, M4, FS);
    private final Map<String, Channel> uriMap = Maps.uniqueIndex(channels, Identified.TO_URI);
    private final Map<String, Channel> keyMap = Maps.uniqueIndex(channels, Channel.TO_KEY);
    
    @Override
    public Maybe<Channel> fromKey(String key) {
        return Maybe.fromPossibleNullValue(keyMap.get(key));
    }

    @Override
    public Maybe<Channel> fromId(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Maybe<Channel> fromUri(String uri) {
        return Maybe.fromPossibleNullValue(uriMap.get(uri));
    }

    @Override
    public Iterable<Channel> forIds(Iterable<Long> ids) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Channel> all() {
        return channels;
    }

    @Override
    public Maybe<Channel> forAlias(String alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Channel> allChannels(ChannelQuery query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Channel> forKeyPairAlias(ChannelQuery query) {
        throw new UnsupportedOperationException();
    }
}
