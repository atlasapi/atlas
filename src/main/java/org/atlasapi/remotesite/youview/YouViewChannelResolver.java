package org.atlasapi.remotesite.youview;

import com.google.common.collect.Multimap;
import org.atlasapi.media.channel.Channel;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

public interface YouViewChannelResolver {

    /** @deprecated only used for tests now */
    @Deprecated Collection<String> getChannelUris(int serviceId);

    /** @deprecated only used for tests now */
    @Deprecated Collection<Channel> getChannels(int serviceId);
    
    Collection<Channel> getAllChannels();

    Multimap<ServiceId, Channel> getAllServiceIdsToChannels();

    Collection<ServiceId> getServiceIds(Channel channel);

    /** A service ID is, by definition, something extracted from an Alias URL. So lets define
     *  the equality behaviour here, and leave the rest to the implementation. */
    abstract class ServiceId {
        public abstract int getId();
        public abstract String getPrefix();
        public abstract String getAlias();

        @Override public final boolean equals(@Nullable Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (!(obj instanceof ServiceId)) return false;
            ServiceId that = (ServiceId) obj;
            if (Objects.equals(this.getAlias(), that.getAlias())) {
                assert (this.getId() == that.getId())
                        : "ServiceId Aliases match (" + this.getAlias() + ")"
                            + " but IDs don't: " + this.getId() + ", " + that.getId();
                assert (Objects.equals(this.getPrefix(), that.getPrefix()))
                        : "ServiceId Aliases match (" + this.getAlias() + ")"
                            + " but Prefixes don't: " + this.getPrefix() + ", " + that.getPrefix();
                return true;
            } else {
                assert (this.getId() != that.getId()
                            || !Objects.equals(this.getPrefix(), that.getPrefix()))
                        : "ServiceId Aliases don't match"
                            + " (" + this.getAlias() + ", " + that.getAlias() + ")"
                            + " but prefixes and IDs do: " + this.getPrefix() + ", " + this.getId();
                return false;
            }
        }
        @Override public final int hashCode() {
            return this.getAlias().hashCode();
        }
        @Override public String toString() {
            return "ServiceId{" + this.getAlias() + "}";
        }
    }
}
