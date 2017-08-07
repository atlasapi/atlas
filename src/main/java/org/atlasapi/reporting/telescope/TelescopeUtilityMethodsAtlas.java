package org.atlasapi.reporting.telescope;

import java.util.Set;

import com.metabroadcast.common.stream.MoreCollectors;
import com.google.common.collect.ImmutableList;
import telescope_client_shaded.com.metabroadcast.columbus.telescope.api.Alias;

public class TelescopeUtilityMethodsAtlas extends  TelescopeUtilityMethods{

    public static ImmutableList<Alias> getAliases(Set<org.atlasapi.media.entity.Alias> aliases) {
        return aliases.stream()
                .map(alias -> Alias.create(alias.getNamespace(), alias.getValue()))
                .collect(MoreCollectors.toImmutableList());
    }
}
