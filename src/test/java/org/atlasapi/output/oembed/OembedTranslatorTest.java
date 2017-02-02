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

package org.atlasapi.output.oembed;

import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.oembed.OembedTranslator.OutputFactory;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

/**
 * Unit test for {@link OembedTranslator}.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
@RunWith(JMock.class)
public class OembedTranslatorTest extends TestCase {
	
	private final Mockery context = new Mockery();
	
	private final Set<Content> graph = Sets.<Content>newHashSet();
	
	private final OutputFactory outputFactory = context.mock(OutputFactory.class);
	private final OembedOutput oembedOutput = context.mock(OembedOutput.class);

	private StubHttpServletRequest request;
	private StubHttpServletResponse response;

	@Override
	@Before
	public void setUp() throws Exception {
		this.request = new StubHttpServletRequest();
		this.response = new StubHttpServletResponse();
	}
	
	@Test
	public void testCreatesFeed() throws Exception {
		
		context.checking(new Expectations() {{ 
			one(outputFactory).createOutput(); 
		}});
		
		new OembedTranslator(outputFactory).writeTo(request, response, graph, ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
	}
	
	@Test
	public void testWritesFeedToStream() throws Exception {
		
		context.checking(new Expectations() {{ 
			allowing(outputFactory).createOutput(); will(returnValue(oembedOutput));
			one(oembedOutput).writeTo(response.getOutputStream());
		}});
		
		new OembedTranslator(outputFactory).writeTo(request, response, graph, ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
	}
	
	@Test
	public void testSetsOembedFieldsFromItemData() throws Exception {
		
		Item item = new Item();
		item.setTitle("Test Title");
		item.setPublisher(Publisher.YOUTUBE);
		item.setCanonicalUri("http://example.com");
		
		Version version = new Version();
		item.addVersion(version);
		
		Encoding encoding = new Encoding();
		encoding.setVideoHorizontalSize(640);
		encoding.setVideoVerticalSize(480);
		version.addManifestedAs(encoding);
		
		Location location = new Location();
		location.setEmbedCode("<embed src=\"a\" />");
		encoding.addAvailableAt(location);
		
		graph.add(item);
		
		context.checking(new Expectations() {{ 
			allowing(outputFactory).createOutput(); will(returnValue(oembedOutput));
			allowing(oembedOutput).writeTo(response.getOutputStream());
	
			one(oembedOutput).setTitle("Test Title");
			one(oembedOutput).setProviderUrl("youtube.com");
			one(oembedOutput).setWidth(640);
			one(oembedOutput).setHeight(480);
			one(oembedOutput).setType("video");
			one(oembedOutput).setEmbedCode("<embed src=\\\"a\\\" />");
		}});
		
		new OembedTranslator(outputFactory).writeTo(request, response, graph, ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
	}

}
