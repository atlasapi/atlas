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

    @Test
    public void titleParsingRemovesWatchPrefixAndOnlinePostfix() {
        Assert.assertEquals("Fair City Extras: Carol and Robbie",
                RteParser.titleParser("Watch Fair City Extras: Carol and Robbie online"));
    }

    @Test
    public void titleParsingRemovesSeasonAndEpisode() {
        Assert.assertEquals("iWitness",
                RteParser.titleParser("Watch iWitness Season 1, Episode 89 online"));
    }

    @Test
    public void checkSeasonInTitleDoesNotFail() {
        Assert.assertEquals("The Word Season In The Middle Of A Title",
                RteParser.titleParser("Watch The Word Season In The Middle Of A Title online"));
    }

    @Test
    public void checkSeasonInTitleWithEpisodeDoesNotFail() {
        Assert.assertEquals("The Word Season Season 54 Episode 24",
                RteParser.titleParser("Watch The Word Season Season 54 Episode 24 online"));
    }
}
