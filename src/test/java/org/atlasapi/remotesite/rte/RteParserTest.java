package org.atlasapi.remotesite.rte;

import org.junit.Assert;
import org.junit.Test;


public class RteParserTest {

    @Test
    public void testCanonicalUriGeneration() {
        Assert.assertEquals("http://rte.ie/shows/123456", RteParser.canonicalUriFrom("uri:avms:123456"));
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfInputIsEmpty() {
        RteParser.canonicalUriFrom("");
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfInputIsNull() {
        RteParser.canonicalUriFrom(null);
    }

    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfUriDoesntHaveIdParam() {
        RteParser.canonicalUriFrom("http://feedurl.com/");
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfUriHasMultipleIdParams() {
        RteParser.canonicalUriFrom("http://feedurl.com/?id=12345&id=56789");
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfUriHasEmptyIdParam() {
        RteParser.canonicalUriFrom("http://feedurl.com/?id=");
    }
    
}
