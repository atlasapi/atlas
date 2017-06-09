package org.atlasapi.output.simple;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.equiv.ChannelRef;

import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelRefSimplifier {

    private final NumberToShortStringCodec codec;

    public ChannelRefSimplifier(NumberToShortStringCodec codec) {
        this.codec = checkNotNull(codec);
    }

    public org.atlasapi.media.entity.simple.ChannelRef simplify(ChannelRef channelRef) {
        return new org.atlasapi.media.entity.simple.ChannelRef(
                codec.encode(BigInteger.valueOf(channelRef.getId())),
                channelRef.getUri(),
                channelRef.getPublisher().key()
        );
    }

    public Set<org.atlasapi.media.entity.simple.ChannelRef> simplify(Set<ChannelRef> channelRefs) {
        return channelRefs.stream()
                .map(this::simplify)
                .collect(Collectors.toSet());
    }

}
