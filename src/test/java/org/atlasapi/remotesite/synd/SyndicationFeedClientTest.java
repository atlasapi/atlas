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

package org.atlasapi.remotesite.synd;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import java.util.List;

import junit.framework.TestCase;

import org.jdom.Element;
import org.junit.Ignore;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;

/**
 * Unit test for {@link SyndicationFeedClient}.
 *
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class SyndicationFeedClientTest extends TestCase {

    @Ignore
    // Test is unstable. Ingoring for now.
	public void testCanReadRssPodcast() throws Exception {

		String feedUrl = "http://downloads.bbc.co.uk/podcasts/radio4/bh/rss.xml";

		SyndFeed feed = new SyndicationFeedClient().get(feedUrl);

		assertThat(feed.getTitle(), is("Broadcasting House"));
		assertThat(feed.getUri(), is(nullValue()));
		assertThat(feed.getLinks(), is(nullValue()));

		List<Element> foreignMarkup = foreignMarkupFrom(feed);
		
		for (Element element : foreignMarkup) {
			if (element.getName().equals("systemRef")) {
				if (element.getAttributeValue("systemId").equals("pid.brand")) {
					assertThat(element.getNamespacePrefix(), is("ppg"));
					assertThat(element.getAttributeValue("key"), startsWith("b00"));
				}
				if (element.getAttributeValue("systemId").equals("pid.genre")) {
					//TODO: find out the correct values for genres
				}
			}
		}
		
		List<SyndEntry> entries = entriesFrom(feed);

		for (SyndEntry syndEntry : entries) {

//			assertThat(syndEntry.getTitle(), containsString("B"));
		//	assertThat(syndEntry.getDescription().getValue(), containsString("Paddy O'Connell"));
			assertThat(syndEntry.getUri(), startsWith("http://downloads.bbc.co.uk/podcasts"));
			assertThat(syndEntry.getLink(), startsWith("http://downloads.bbc.co.uk/podcasts"));
		}
	}

	@SuppressWarnings("unchecked")
	private List<Element> foreignMarkupFrom(SyndFeed feed) {
		return (List<Element>) feed.getForeignMarkup();
	}

	@SuppressWarnings("unchecked")
	private List<SyndEntry> entriesFrom(SyndFeed feed) {
		return feed.getEntries();
	}
}
