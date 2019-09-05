package org.atlasapi.equiv.utils.barb;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class BarbEquivUtils {

    public static final Set<Publisher> TXLOG_PUBLISHERS = ImmutableSet.of(
            Publisher.BARB_TRANSMISSIONS,
            Publisher.LAYER3_TXLOGS
    );
}
