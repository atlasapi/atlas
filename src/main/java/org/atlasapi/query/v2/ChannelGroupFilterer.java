package org.atlasapi.query.v2;

import java.util.Set;

import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupType;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.Region;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class ChannelGroupFilterer {
    
    public Set<ChannelGroup> filter(Iterable<ChannelGroup> channelGroups, ChannelGroupFilter filter) {
        return ImmutableSet.copyOf(Iterables.filter(channelGroups, filterPredicate(filter)));
    }
    
    private Predicate<ChannelGroup> filterPredicate(final ChannelGroupFilter filter) {
        return new Predicate<ChannelGroup>() {
            @Override
            public boolean apply(ChannelGroup input) {
                
                if (filter.type.isPresent() && !filter.type.get().equals(ChannelGroupType.from(input))) {
                    return false;
                }
                
                if (filter.platform.isPresent()) {
                    if (input instanceof Region) {
                        Region region = (Region)input;
                        return filter.platform.get().getId().equals(region.getPlatform());
                    } else {
                        return false;
                    }
                }

                return true;
            }
        };
    }
    
    public static class ChannelGroupFilter {
        private final Optional<ChannelGroupType> type;
        private final Optional<Platform> platform;

        private ChannelGroupFilter(Optional<ChannelGroupType> type, Optional<Platform> platform) {
            this.type = type;
            this.platform = platform;
        }
        
        public static ChannelGroupFilterBuilder builder() {
            return new ChannelGroupFilterBuilder();
        }
        
        public static class ChannelGroupFilterBuilder {
            private Optional<ChannelGroupType> type = Optional.absent();
            private Optional<Platform> platform = Optional.absent();
            
            private ChannelGroupFilterBuilder() {
            }
            
            public ChannelGroupFilter build() {
                return new ChannelGroupFilter(type, platform);
            }
            
            public ChannelGroupFilterBuilder withType(ChannelGroupType type) {
                this.type = Optional.of(type);
                return this;
            }
            
            public ChannelGroupFilterBuilder withPlatform(Platform platform) {
                this.platform = Optional.of(platform);
                return this;
            }
        }
    }
}
