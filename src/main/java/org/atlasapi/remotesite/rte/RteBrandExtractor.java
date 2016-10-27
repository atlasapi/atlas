package org.atlasapi.remotesite.rte;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.RelatedLink.LinkType;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.collect.ImmutableSet;
import nu.xom.Element;
import nu.xom.Elements;


public class RteBrandExtractor implements ContentExtractor<Element, Brand> {

    private final RteParser rteParser;

    private RteBrandExtractor() {
        this.rteParser = RteParser.create();
    }

    public static RteBrandExtractor create() {
        return new RteBrandExtractor();
    }

    @Override
    public Brand extract(Element element) {
        Elements childElements = element.getChildElements();
        Brand brand = new Brand();
        brand.setPublisher(Publisher.RTE);
        brand.setMediaType(MediaType.VIDEO);
        
        for (int i=0; i < childElements.size(); i++) {
            Element childElement = childElements.get(i);
            String attrName = childElement.getAttribute("name").getValue();
            String value = childElement.getValue();
            populateModelFieldFromAttribute(attrName, value, brand);
        }
        
        return brand;
    }

    private void populateModelFieldFromAttribute(String attrName, String value, Brand brand) {
        switch(attrName) {
        case "title":
            brand.setTitle(rteParser.titleParser(value));
            break;
        case "identifier": 
            brand.setCanonicalUri(rteParser.canonicalUriFrom(value));
            break;
        case "url":
            brand.setRelatedLinks(ImmutableSet.of(
                    new RelatedLink.Builder(LinkType.VOD, value).build()
            ));
            break;
        }
    }
}
