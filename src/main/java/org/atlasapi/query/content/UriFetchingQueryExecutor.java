/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.query.content;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.system.Fetcher;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Finds any uris from a given {@link ContentQuery}, fetches them using a local/remote
 * fetcher (so either from the database or from the Internet), and uses the response
 * to replace the uris given in the query with the canonical versions of each, before passing
 * the updated query on to a delegate. 
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class UriFetchingQueryExecutor implements KnownTypeQueryExecutor {

    private static final Function<Identified, Set<String>> TO_ALL_URIS = new Function<Identified, Set<String>>() {
        @Override
        public Set<String> apply(@Nullable Identified input) {
            return input.getAllUris();
        }
    };
    
	private final Fetcher<Identified> fetcher;
	private final KnownTypeQueryExecutor delegate;
    private final EquivalenceUpdater<Content> equivUpdater;
    private final Set<Publisher> equivalablePublishers;
	
	public UriFetchingQueryExecutor(Fetcher<Identified> fetcher, KnownTypeQueryExecutor delegate, EquivalenceUpdater<Content> equivUpdater, Set<Publisher> equivalablePublishers) {
		this.fetcher = fetcher;
		this.delegate = delegate;
        this.equivUpdater = equivUpdater;
        this.equivalablePublishers = equivalablePublishers;
	}
	
	@Override
	public Map<Id, List<Identified>> executeUriQuery(Iterable<String> uris, ContentQuery query) {
		return executeContentQuery(uris, query);
	}

	@Override
	public Map<Id, List<Identified>> executeIdQuery(Iterable<Id> ids, ContentQuery query) {
	    return delegate.executeIdQuery(ids, query);
	}
	
	public Map<Id, List<Identified>> executeContentQuery(Iterable<String> uris, ContentQuery query) {

		Map<Id, List<Identified>> found = delegate.executeUriQuery(uris, query);
		
		Set<String> missingUris = missingUris(Iterables.concat(Iterables.transform(Iterables.concat(found.values()),TO_ALL_URIS)), uris);
		
		if (missingUris.isEmpty()) {
			return found;
		} 

		Map<String, Identified> fetched = Maps.newHashMap();
		Map<Id, List<Identified>> youtubeContentGroups = Maps.newHashMap();
		
		for (String missingUri : missingUris) {
			Identified remoteContent = fetcher.fetch(missingUri);
			if (remoteContent != null) {
			    if (remoteContent instanceof ContentGroup && ((ContentGroup) remoteContent).getPublisher().equals(Publisher.YOUTUBE)) {
			        youtubeContentGroups.put(remoteContent.getId(), ImmutableList.of(remoteContent));
			    } else {
			        fetched.put(remoteContent.getCanonicalUri(), remoteContent);
			    }
			}
		}

		Builder<Id, List<Identified>> results = ImmutableMap.<Id, List<Identified>>builder().putAll(found).putAll(youtubeContentGroups);
		
		// If we couldn't resolve any of the missing uris then we should just return the results of the original query
		if (fetched.isEmpty()) {
            return results.build();
		}
		
		updateEquivalences(fetched);
		
		// re-attempt the query now the missing uris have been fetched
		return results.putAll(delegate.executeUriQuery(fetched.keySet(), query)).build();
	}

    private void updateEquivalences(Map<String, Identified> fetched) {
        for (Identified fetchedEntity : fetched.values()) {
		    if (fetchedEntity instanceof Content) {
		        Content fetchedContent = (Content) fetchedEntity;
                if (equivalablePublishers.contains(fetchedContent.getPublisher())) {
                    equivUpdater.updateEquivalences(fetchedContent);
                }
            }
        }
    }
	
	private static Set<String> missingUris(Iterable<String> content, Iterable<String> uris) {
		return Sets.difference(ImmutableSet.copyOf(uris), ImmutableSet.copyOf(content));
	}
}
