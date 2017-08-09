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

package org.atlasapi.remotesite;

import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.reporting.telescope.OwlTelescopeProxy;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import com.metabroadcast.columbus.telescope.api.Event;

import com.google.common.collect.Sets;

public class ContentWriters implements ContentWriter {

	private final OwlTelescopeProxy telescopeProxy;
	private Set<ContentWriter> writers = Sets.newHashSet();

	public ContentWriters() {
		telescopeProxy = OwlTelescopeProxy.create(
				OwlTelescopeReporters.EQUIVALENCE,
				Event.Type.EQUIVALENCE
		);

		telescopeProxy.startReporting();
	}
	
	@Override
	public Item createOrUpdate(Item item, OwlTelescopeProxy telescopeProxy) {
		for (ContentWriter writer : writers) {
			writer.createOrUpdate(item, telescopeProxy);
		}
        return item;
	}
	
	public void add(ContentWriter writer) {
		this.writers.add(writer);
	}

	@Override
	public void createOrUpdate(Container container, OwlTelescopeProxy telescopeProxy) {
		for (ContentWriter writer : writers) {
			writer.createOrUpdate(container, telescopeProxy);
		}
	}
}
