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

package org.atlasapi.output;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.atlasapi.media.entity.testing.ItemTestDataBuilder.item;
import static org.atlasapi.media.entity.testing.PlaylistTestDataBuilder.playlist;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;

import java.io.IOException;

import junit.framework.TestCase;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.ParsingException;
import nu.xom.ValidityException;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.ContentIdentifier;
import org.atlasapi.media.entity.simple.ContentQueryResult;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.KeyPhrase;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.media.entity.simple.PublisherDetails;
import org.atlasapi.media.entity.simple.RelatedLink;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

/**
 * 
 * @author Fred van den Driessche (fred@metabroadcast.com)
 *
 */
public class JaxbXmlTranslatorTest extends TestCase {

    private final JaxbXmlTranslator<ContentQueryResult> translator = new JaxbXmlTranslator<ContentQueryResult>();
    
	private StubHttpServletRequest request;
	private StubHttpServletResponse response;

	@Override
	public void setUp() throws Exception {
		this.request = new StubHttpServletRequest();
		this.response = new StubHttpServletResponse();
	}

    @Test
	public void testCanOutputSimpleItemObjectModelIdentifiedFieldsAsXml() throws Exception {
	    
		Item item = item().build();
		
		Document outputDoc = serializeToXml(new ContentQueryResult(item));

		
		Element root = outputDoc.getRootElement();
		assertThat(root, allOf(of(localName(is("content")), namespacePrefix(is("play")))));
        
        Element itemElem = root.getChildElements().get(0);
        assertThat(itemElem, allOf(of(localName(is("item")), namespacePrefix(is("play")))));
        
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("uri")), value(is(item.getUri()))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("curie")), value(is(item.getCurie()))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("id")), value(is(item.getId()))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("type")), value(is(item.getType()))))));
        
	}

    @Test
	public void testCanOutputSimpleItemObjectModelAliasedFieldsAsXml() throws Exception {
        
        Item item = item().withAliases("thisisanalias").build();
        
        Document outputDoc = serializeToXml(new ContentQueryResult(item));
        
        Element itemElem = outputDoc.getRootElement().getChildElements().get(0);
        
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("aliases")),hasChildElem(allOf(of(localName(is("alias")),value(is(getOnlyElement(item.getAliases()))))))))));
		
	}

    @Test
	public void testCanOutputSimpleItemObjectModelDescriptionFieldsAsXml() throws Exception {
        
        Item item = item().build();
        
        Document outputDoc = serializeToXml(new ContentQueryResult(item));
        
        Element itemElem = outputDoc.getRootElement().getChildElements().get(0);
        
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("title")),value(is(String.valueOf(item.getTitle())))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("description")),value(is(item.getDescription()))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("image")),value(is(item.getImage()))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("thumbnail")),value(is(String.valueOf(item.getThumbnail())))))));
        
        assertThat(itemElem, hasChildElem(allOf(of(
                localName(is("genres")),
                namespacePrefix(is("play")),
                hasChildElem(allOf(of(localName(is("genre")),value(is(getOnlyElement(item.getGenres()))))))
        ))));

        assertThat(itemElem, hasChildElem(allOf(of(
                localName(is("publisher")),
                hasChildElem(allOf(of(localName(is("country")),value(is(item.getPublisher().getCountry()))))),
                hasChildElem(allOf(of(localName(is("key")),value(is(item.getPublisher().getKey()))))),
                hasChildElem(allOf(of(localName(is("name")),value(is(item.getPublisher().getName())))))
        ))));
        
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("scheduleOnly")),value(is(String.valueOf(item.isScheduleOnly())))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("specialization")),value(is(item.getSpecialization().toString().toLowerCase()))))));

        //TODO: clips, key phrases, tags, sameAs, mediaType, topics, presentation channel
        
	}

    @Test
	public void testCanOutputSimpleItemObjectModelItemFieldsAsXml() throws Exception {
        
        Item item = item().build();
        
        Document outputDoc = serializeToXml(new ContentQueryResult(item));
        
        Element itemElem = outputDoc.getRootElement().getChildElements().get(0);
	    
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("episodeNumber")),value(is(String.valueOf(item.getEpisodeNumber())))))));
        assertThat(itemElem, hasChildElem(allOf(of(localName(is("seriesNumber")),value(is(String.valueOf(item.getSeriesNumber())))))));
        
        assertThat(itemElem, hasChildElem(allOf(of(
                localName(is("container")),
                hasChildElem(allOf(of(localName(is("curie")),value(is(item.getBrandSummary().getCurie()))))),
                hasChildElem(allOf(of(localName(is("uri")),value(is(item.getBrandSummary().getUri()))))),
                hasChildElem(allOf(of(localName(is("description")),value(is(item.getBrandSummary().getDescription()))))),
                hasChildElem(allOf(of(localName(is("title")),value(is(item.getBrandSummary().getTitle())))))
        ))));
        
        assertThat(itemElem, hasChildElem(allOf(of(
                localName(is("seriesSummary")),
                hasChildElem(allOf(of(localName(is("curie")),value(is(item.getSeriesSummary().getCurie()))))),
                hasChildElem(allOf(of(localName(is("uri")),value(is(item.getSeriesSummary().getUri()))))),
                hasChildElem(allOf(of(localName(is("description")),value(is(item.getSeriesSummary().getDescription()))))),
                hasChildElem(allOf(of(localName(is("title")),value(is(item.getSeriesSummary().getTitle())))))
        ))));
        
        //TODO: locations, broadcasts, people, blackAndWhite, countriesOfOrigin, year
	}

    @Test
    public void testCanOutputSimeItemObjectModelItemWithKeyPhrasesAsXml() throws Exception {
        KeyPhrase phrase = new KeyPhrase("phrase",new PublisherDetails(Publisher.BBC.key()),1.0);
        Item item = item().withKeyPhrases(ImmutableSet.of(phrase)).build();

        Document outputDoc = serializeToXml(new ContentQueryResult(item));

        Element itemElem = outputDoc.getRootElement().getChildElements().get(0);
        
        assertThat(itemElem, hasChildElem(allOf(of(
                localName(is("key_phrases")),
                hasChildElem(allOf(of(
                        localName(is("key_phrase")),
                        hasChildElem(allOf(of(
                                localName(is("phrase")),
                                value(is(phrase.getPhrase()))
                        ))),
                        hasChildElem(allOf(of(
                                localName(is("weighting")),
                                value(is(phrase.getWeighting().toString()))
                        )))
                )))
        ))));
    }

    @Test
    public void testCanOutputSimeItemObjectModelItemWithRelatedLinksAsXml() throws Exception {
        RelatedLink link = new RelatedLink();
        link.setType("facebook");
        link.setUrl("http://www.facebook.com/relatedlink");
        link.setTitle("Related Link");
        
        Item item = item().withRelatedLinks(ImmutableSet.of(link)).build();
        
        Document outputDoc = serializeToXml(new ContentQueryResult(item));

        Element itemElem = outputDoc.getRootElement().getChildElements().get(0);
        
        assertThat(itemElem, hasChildElem(allOf(of(
                localName(is("related_links")),
                hasChildElem(allOf(of(
                        localName(is("related_link")),
                        hasChildElem(allOf(of(
                                localName(is("url")),
                                value(is(link.getUrl()))
                        ))),
                        hasChildElem(allOf(of(
                                localName(is("title")),
                                value(is(link.getTitle()))
                        ))),
                        hasChildElem(allOf(of(
                                localName(is("type")),
                                value(is(link.getType()))
                        )))
                )))
        ))));
    }

    @Test
	public void testCanOutputSimpleListObjectModelPlaylistFieldsAsXml() throws Exception {

		Playlist list = playlist().build();

		list.add(ContentIdentifier.identifierFrom("dd", "http://www.bbc.co.uk/bluepeter", "item"));
		
        Document outputDoc = serializeToXml(new ContentQueryResult(list));
        
        Element listElem = outputDoc.getRootElement().getChildElements().get(0);
        assertThat(listElem, allOf(of(localName(is("playlist")), namespacePrefix(is("play")))));
        
        assertThat(listElem, hasChildElem(allOf(of(
                localName(is("content")),
                hasChildElem(allOf(of(
                        localName(is("item")),
                        hasChildElem(allOf(of(localName(is("type")), value(is("item"))))),
                        hasChildElem(allOf(of(localName(is("id")), value(is("dd"))))),
                        hasChildElem(allOf(of(localName(is("uri")), value(is(getOnlyElement(list.getContent()).getUri())))))
                )))
        ))));
        
	}

    private Document serializeToXml(ContentQueryResult result) throws IOException, ParsingException, ValidityException {
        translator.writeTo(request, response, result, ImmutableSet.copyOf(Annotation.values()), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        String output = response.getResponseAsString();

        Document outputDoc = new Builder().build(output, "http://ref.atlasapi.org/");
        return outputDoc;
    }
    
    private Matcher<? extends Element> localName(final Matcher<String> nameMatcher) {
        return new TypeSafeMatcher<Element>() {
            
            @Override
            public void describeTo(org.hamcrest.Description desc) {
                desc.appendValue("Element with ").appendDescriptionOf(nameMatcher);
            }

            @Override
            public boolean matchesSafely(Element elem) {
                return nameMatcher.matches(elem.getLocalName());
            }
        };
    }
    
    private Matcher<? extends Element> namespacePrefix(final Matcher<String> namespaceMatcher) {
        return new TypeSafeMatcher<Element>() {

            @Override
            public void describeTo(org.hamcrest.Description desc) {
                desc.appendValue("Element with ").appendDescriptionOf(namespaceMatcher);
            }

            @Override
            public boolean matchesSafely(Element elem) {
                return namespaceMatcher.matches(elem.getNamespacePrefix());
            }
        };
    }
    
    private Matcher<? extends Element> value(final Matcher<String> contentMatcher) {
        return new TypeSafeMatcher<Element>() {

            @Override
            public void describeTo(org.hamcrest.Description desc) {
                desc.appendValue("Element with ").appendDescriptionOf(contentMatcher);
            }

            @Override
            public boolean matchesSafely(Element elem) {
                return contentMatcher.matches(elem.getValue());
            }
        };
    }
    
    private Matcher<Element> hasChildElem(final Matcher<? extends Element> elemMatcher) {
        return new TypeSafeMatcher<Element>() {

            @Override
            public void describeTo(org.hamcrest.Description desc) {
                desc.appendValue("Element with ").appendDescriptionOf(elemMatcher);
            }

            @Override
            public boolean matchesSafely(Element elem) {
                for (int i = 0; i < elem.getChildCount(); i++) {
                    if (elemMatcher.matches(elem.getChildElements().get(i))) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
    
//    Serializer serializer = new Serializer(System.out, Charsets.UTF_8.toString());
//    serializer.setLineSeparator("\n");
//    serializer.setIndent(4);
//    serializer.write(new Document(outputDoc));
}
