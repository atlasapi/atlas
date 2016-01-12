/* Copyright 2009 British Broadcasting Corporation
   Copyright 2009 Meta Broadcast Ltd

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

import junit.framework.TestCase;

import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.query.v2.QueryController;
import org.junit.Test;

import com.metabroadcast.common.properties.Configurer;
import org.junit.Ignore;

/**
 * Test that we can load beans from the Spring configuration - checks that the
 * config is wired correctly.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
@Ignore
public class SpringTest extends TestCase {

    @Test
    public void testCanCreateQueryController() throws Exception {
        System.setProperty(Configurer.PLATFORM_VARIABLE, "dev");
        Configurer.load();
        ConfigurableAnnotationWebApplicationContext applicationContext = new ConfigurableAnnotationWebApplicationContext();
        applicationContext.setConfigLocation(null);
        applicationContext.refresh();
        applicationContext.getBean(QueryController.class);
    }

    @Test
    public void testCanCreateProcessing() throws Exception {
        System.setProperty("processing.config", "true");
        ConfigurableAnnotationWebApplicationContext applicationContext = new ConfigurableAnnotationWebApplicationContext();
        applicationContext.setConfigLocation(null);
        applicationContext.refresh();
        applicationContext.getBean(MongoPersonStore.class);
    }
}
