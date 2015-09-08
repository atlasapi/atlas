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

package org.atlasapi.query.uri;


import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.system.Fetcher;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;

/**
 * Aggregate {@link Fetcher} that checks a local datastore for resources,
 * before trying to fetch them remotely.
 *
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class LocalOrRemoteFetcher implements Fetcher<Identified>, ContentResolver {

	private final ContentResolver localStore;
	private final Fetcher<Identified> remoteFetcher;
	
	public LocalOrRemoteFetcher(ContentResolver localStore, Fetcher<Identified> remoteFetcher) {
		this.localStore = localStore;
		this.remoteFetcher = remoteFetcher;
	}

	public Identified fetch(String uri) {
		
		Maybe<Identified> local = localStore.findByCanonicalUris(ImmutableList.of(uri)).get(uri);
		
		if (local.isNothing()) {
			return remoteFetcher.fetch(uri); 
		} else {
		    return local.requireValue();
		}
	}

	@Override
	public ResolvedContent findByCanonicalUris(Iterable<? extends String> canonicalUris) {
		ResolvedContentBuilder builder = ResolvedContent.builder();
		for (String uri : canonicalUris) {
			builder.put(uri, fetch(uri));
		}
		return builder.build();
	}

	@Override
    public ResolvedContent findByUris(Iterable<String> uris) {
        throw new UnsupportedOperationException();
    }
}
