package org.atlasapi.remotesite.pa.channels;

import static com.google.api.client.util.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;


public class ChannelNumberingFilterer {

    private final Logger log = LoggerFactory.getLogger(ChannelNumberingFilterer.class);
    private final ChannelGroupResolver channelGroupResolver;
    private final LoadingCache<Long, Publisher> groupPublisherCache = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<Long, Publisher>() {
                @Override
                public Publisher load(Long key) throws Exception {
                    Optional<ChannelGroup> group = channelGroupResolver.channelGroupFor(key);
                    if (group.isPresent()) {
                        return group.get().getPublisher();
                    }
                    return null;
                }
            });
    
    public ChannelNumberingFilterer(ChannelGroupResolver channelGroupResolver) {
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
    }

    public Iterable<ChannelNumbering> filterNotEqualToGroupPublisher(Iterable<ChannelNumbering> numberings, final Publisher publisher) {
        return Iterables.filter(numberings, new Predicate<ChannelNumbering>() {
            @Override
            public boolean apply(ChannelNumbering input) {
                try {
                    Publisher groupPublisher = groupPublisherCache.get(input.getChannelGroup());
                    if (groupPublisher == null) {
                        return false;
                    }
                    return !publisher.equals(groupPublisher);
                } catch (ExecutionException e) {
                    log.error("Exception upon fetch of Publisher for Channel Group " + input.getChannelGroup(), e);
                    return true;
                }
            }
        });
    }
}
