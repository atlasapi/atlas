package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.base.Objects;
import org.atlasapi.media.channel.Channel;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;


public class ChannelDay {

    private final Channel channel;
    private final LocalDate day;

    public ChannelDay(Channel channel, LocalDate day) {
        this.channel = checkNotNull(channel);
        this.day = checkNotNull(day);
    }
    
    public Channel getChannel() {
        return channel;
    }

    public LocalDate getDay() {
        return day;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ChannelDay) {
            ChannelDay other = (ChannelDay) that;
            return day.equals(other.day)
                && channel.equals(other.channel);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(day, channel);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("channel", channel)
                .add("day", day)
                .toString();
    }
    
}
