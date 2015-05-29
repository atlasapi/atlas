package org.atlasapi.remotesite.rte;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import nu.xom.Attribute;
import nu.xom.Element;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.RelatedLink.LinkType;
import org.junit.Test;


public class RteBrandExtractorTest {
    
    private static final String TITLE = "A Stranger's Notebook on Dublin";

    private static final String PLAYER_URI = "http://www.rte.ie/player/ie/show/10417462/";
    
    private final RteBrandExtractor extractor = new RteBrandExtractor();
    
    @Test
    public void testBrandExtraction() {
        Element element = new Element("doc");
        
        appendStringElement(element, "title", TITLE);
        appendStringElement(element, "identifier", "uri:avms:10417462");
        appendStringElement(element, "url", PLAYER_URI);
        
        Brand brand = extractor.extract(element);
        
        assertThat(brand.getTitle(), equalTo(TITLE));
        assertThat(brand.getCanonicalUri(), equalTo("http://rte.ie/shows/10417462"));
        assertThat(brand.getPublisher(), equalTo(Publisher.RTE));
        assertThat(brand.getMediaType(), equalTo(MediaType.VIDEO));
        
        RelatedLink relatedLink = new RelatedLink.Builder(LinkType.VOD, PLAYER_URI).build();
        assertThat(brand.getRelatedLinks().size(), equalTo(1));
        assertThat(brand.getRelatedLinks(), hasItem(relatedLink));
    }

    private void appendStringElement(Element element, String title, String value) {
        Element stringElement = new Element("str");
        stringElement.addAttribute(new Attribute("name", title));
        stringElement.appendChild(value);
        element.appendChild(stringElement);
        
    }
    
}
