package org.atlasapi.remotesite.rte;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class RteParserTest {
    
    private RteParser rteParser;

    @Before
    public void setUp() throws Exception {
        rteParser = RteParser.create();
    }

    @Test
    public void testCanonicalUriGeneration() {
        Assert.assertEquals("http://rte.ie/shows/123456", rteParser.canonicalUriFrom("uri:avms:123456"));
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfInputIsEmpty() {
        rteParser.canonicalUriFrom("");
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfInputIsNull() {
        //noinspection ConstantConditions
        rteParser.canonicalUriFrom(null);
    }

    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfUriDoesntHaveIdParam() {
        rteParser.canonicalUriFrom("http://feedurl.com/");
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfUriHasMultipleIdParams() {
        rteParser.canonicalUriFrom("http://feedurl.com/?id=12345&id=56789");
    }
    
    @Test(expected=IllegalArgumentException.class) 
    public void canonicalUriGenerationShouldFailIfUriHasEmptyIdParam() {
        rteParser.canonicalUriFrom("http://feedurl.com/?id=");
    }

    @Test
    public void titleParsingRemovesWatchPrefixAndOnlinePostfix() {
        Assert.assertEquals("Fair City Extras: Carol and Robbie",
                rteParser.titleParser("Watch Fair City Extras: Carol and Robbie online"));
    }

    @Test
    public void titleParsingRemovesSeasonAndEpisode() {
        Assert.assertEquals("iWitness",
                rteParser.titleParser("Watch iWitness Season 1, Episode 89 online"));
    }

    @Test
    public void checkSeasonInTitleDoesNotFail() {
        Assert.assertEquals("The Word Season In The Middle Of A Title",
                rteParser.titleParser("Watch The Word Season In The Middle Of A Title online"));
    }

    @Test
    public void checkSeasonInTitleWithEpisodeDoesNotFail() {
        Assert.assertEquals("The Word Season Season 54 Episode 24",
                rteParser.titleParser("Watch The Word Season Season 54 Episode 24 online"));
    }

    @Test
    public void checkFilmsEndingWithOnRtePlayer() {
        Assert.assertEquals("Big School",
                rteParser.titleParser("Watch Big School on RT&Eacute; Player"));
    }

    @Test
    public void checkLogger() {
        Assert.assertEquals("Something that doesn't match anything",
                rteParser.titleParser("Something that doesn't match anything"));
    }

    @Test
    public void checkDoesNotFailIfPostfixMissing() {
        Assert.assertEquals("something that doesn't have a matching end",
                rteParser.titleParser("Watch something that doesn't have a matching end"));
    }
}
