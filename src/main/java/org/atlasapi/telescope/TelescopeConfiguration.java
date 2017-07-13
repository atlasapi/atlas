package org.atlasapi.telescope;

import com.metabroadcast.common.properties.Configurer;

/**
 * TODO: This file is probably generic for atlas, and does not need to be in BBC nitro.
 *
 * @author andreas
 */
public class TelescopeConfiguration {
    public static final String TELESCOPE_HOST = Configurer.get("telescope.host").get();
    public static final String ENVIRONMENT = Configurer.get("telescope.environment").get();
}
