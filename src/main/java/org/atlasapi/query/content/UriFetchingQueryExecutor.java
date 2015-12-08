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
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.system.Fetcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
            return Sets.union(input.getAllUris(), ImmutableSet.copyOf(
                    Iterables.transform(input.getEquivalentTo(), LookupRef.TO_URI)));
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
	public Map<String, List<Identified>> executeUriQuery(Iterable<String> uris, ContentQuery query) {
		return executeContentQuery(uris, query);
	}

	@Override
	public Map<String, List<Identified>> executeIdQuery(Iterable<Long> ids, ContentQuery query) {
	    return delegate.executeIdQuery(ids, query);
	}

    @Override
    public Map<String, List<Identified>> executeAliasQuery(Optional<String> namespace, Iterable<String> values,
            ContentQuery query) {
        return delegate.executeAliasQuery(namespace, values, query);
    }
    
    @Override
    public Map<String, List<Identified>> executePublisherQuery(Iterable<Publisher> publishers,
            ContentQuery query) {
        return delegate.executePublisherQuery(publishers, query);
    }

	@Override
	public Map<String, List<Identified>> executeEventQuery(Iterable<Long> eventIds,
			ContentQuery query) {
		return delegate.executeEventQuery(eventIds, query);
	}

	public Map<String, List<Identified>> executeContentQuery(Iterable<String> uris, ContentQuery query) {

		Map<String, List<Identified>> found = delegate.executeUriQuery(uris, query);
		
		Set<String> missingUris = missingUris(Iterables.concat(Iterables.transform(Iterables.concat(found.values()),TO_ALL_URIS)), uris);
		
		if (missingUris.isEmpty()) {
			return found;
		} 

		Map<String, Identified> fetched = Maps.newHashMap();
		Map<String, List<Identified>> youtubeContentGroups = Maps.newHashMap();
		
		for (String missingUri : missingUris) {
			Identified remoteContent = fetcher.fetch(missingUri);
			if (remoteContent != null) {
			    if (remoteContent instanceof ContentGroup && ((ContentGroup) remoteContent).getPublisher().equals(Publisher.YOUTUBE)) {
			        youtubeContentGroups.put(missingUri, ImmutableList.of(remoteContent));
			    } else {
			        fetched.put(remoteContent.getCanonicalUri(), remoteContent);
			    }
			}
		}

		Map<String, List<Identified>> results = Maps.newHashMap();
		results.putAll(found);
		results.putAll(youtubeContentGroups);
		
		// If we couldn't resolve any of the missing uris then we should just return the results of the original query
		if (fetched.isEmpty()) {
            return results;
		}
		
		updateEquivalences(fetched);
		
		// re-attempt the query now the missing uris have been fetched
		results.putAll(delegate.executeUriQuery(fetched.keySet(), query));
		return results;
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
