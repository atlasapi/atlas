/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.webapp.properties.ContextConfigurer;
import org.atlasapi.system.JettyHealthProbe;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AtlasModule {
    
    public @Bean HealthProbe jettyHealthProbe() {
        return new JettyHealthProbe();
    }

	public @Bean ContextConfigurer config() {
		ContextConfigurer c = new ContextConfigurer();
		c.init();
		return c;
	}
}
