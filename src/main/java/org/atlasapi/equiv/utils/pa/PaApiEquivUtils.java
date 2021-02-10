package org.atlasapi.equiv.utils.pa;

import com.google.common.collect.ImmutableSet;

public class PaApiEquivUtils {

    public static final ImmutableSet<String> ITEM_LEGACY_PA_ID_ALIAS_NAMESPACES = ImmutableSet.of(
            "pa:episode", "pa:film", "pa:api:asset:legacy_id"
    );

    public static final ImmutableSet<String> SERIES_LEGACY_PA_ID_ALIAS_NAMESPACES = ImmutableSet.of(
            "pa:series", "pa:api:asset:legacy_id"
    );

    public static final ImmutableSet<String> CONTAINER_LEGACY_PA_ID_ALIAS_NAMESPACES = ImmutableSet.of(
            "pa:brand", "pa:api:asset:legacy_id"
    );

}
