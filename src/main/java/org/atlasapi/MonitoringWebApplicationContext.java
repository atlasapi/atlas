package org.atlasapi;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.atlasapi.logging.HealthModule;
import org.atlasapi.system.JettyHealthProbe;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;


public class MonitoringWebApplicationContext extends AnnotationConfigWebApplicationContext {

    private static final Function<Class<?>, String> TO_FQN = Class::getCanonicalName;

    @Override
    public final void setConfigLocation(String location) {
        super.setConfigLocations(
                Lists.transform(
                        ImmutableList.of(
                                JettyHealthProbe.class,
                                HealthModule.class
                        ),
                        TO_FQN
                ).toArray(new String[0])
        );
    }
    
    
}
