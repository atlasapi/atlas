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

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;

import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.system.Fetcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 */
@SuppressWarnings("unchecked")
@RunWith(JMock.class)
public class UriFetchingQueryExecutorTest extends TestCase {

	private static final ContentQuery A_FILTER = ContentQuery.MATCHES_EVERYTHING;
	
	private static final Episode item1 = new Episode("item1", "curie:1", Publisher.BBC);
	private static final Episode item2 = new Episode("item2", "curie:2", Publisher.BBC);

    private final Mockery context = new Mockery();
	private final Fetcher<Identified> fetcher = context.mock(Fetcher.class);
	private final KnownTypeQueryExecutor delegate = context.mock(KnownTypeQueryExecutor.class);
	private final EquivalenceUpdater<Content> equivUpdater = context.mock(EquivalenceUpdater.class);
	private final Set<Publisher> publishers = ImmutableSet.of();
	
	private UriFetchingQueryExecutor executor = new UriFetchingQueryExecutor(fetcher, delegate,equivUpdater, publishers);

    @Test
	public void testThatWhenTheQueryIsSatisfiedByTheDatabaseThatTheFetcherIsNotUsed() throws Exception {
		
		context.checking(new Expectations() {{ 
			one(delegate).executeUriQuery(ImmutableList.of("item1"), A_FILTER); will(returnValue(uriContentMapFor(item1)));
			never(fetcher);
		}});
		
		executor.executeUriQuery(ImmutableList.of("item1"), A_FILTER);
	}

    @Test
	public void testThatWhenTheQueryIsNotSatisfiedByTheDatabaseTheFetcherIsUsed() throws Exception {
		
		context.checking(new Expectations() {{ 
			one(delegate).executeUriQuery(ImmutableList.of("item1"), A_FILTER); will(returnValue(ImmutableMap.<String,List<Identified>>of()));
			one(fetcher).fetch("item1"); will(returnValue(item1));
			
			// the result of the fetcher cannot be returned directly because further view restrictions / filters
			// need to be applied -- so the query is retried with the item now loaded into the db
			one(delegate).executeUriQuery(with(hasItem("item1")), with(A_FILTER)); will(returnValue(uriContentMapFor(item1)));
		}});
		
		executor.executeUriQuery(ImmutableList.of("item1"), A_FILTER);
	}

    @Test
	public void testThatIfTheFetcherReturnsNothingTheTheDBIsNotRetried() throws Exception {
		
		context.checking(new Expectations() {{ 
			one(delegate).executeUriQuery(ImmutableList.of("item1"), A_FILTER); will(returnValue(ImmutableMap.of()));
			one(fetcher).fetch("item1"); will(returnValue(null));

		}});

		executor.executeUriQuery(ImmutableList.of("item1"), A_FILTER);
	}

    @Test
	public void testThatWhenSomeItemsAreInTheDatabaseAndSomeAreNotThatTheFetcherIsUsedOnTheMissingItems() throws Exception {
		final List<String> urisOfItems1And2 = ImmutableList.of("item1", "item2");
		
		context.checking(new Expectations() {{ 
			one(delegate).executeUriQuery(urisOfItems1And2, A_FILTER); will(returnValue(uriContentMapFor(item1)));
			one(fetcher).fetch("item2"); will(returnValue(item2));
			one(delegate).executeUriQuery(with(hasItems("item2")), with(A_FILTER)); //will(returnValue(ImmutableList.of(item1, item2)));
		}});

		executor.executeUriQuery(urisOfItems1And2, A_FILTER);
	}


    private Map<String, List<Identified>> uriContentMapFor(Episode item) {
        return ImmutableMap.<String, List<Identified>>of(item.getCanonicalUri(),ImmutableList.<Identified>of(item));
    }
    
}
