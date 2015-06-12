package org.atlasapi.remotesite.knowledgemotion.topics.spotlight;

import static org.junit.Assert.assertTrue;

import org.atlasapi.remotesite.knowledgemotion.topics.KeywordsExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.WikipediaKeyword;
import org.atlasapi.remotesite.knowledgemotion.topics.spotlight.SpotlightKeywordsExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.spotlight.SpotlightResourceParser;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Optional;

public class SpotlightIntegrationTest {
    @Test
    public void testSpotlightIntegrationForSingleKwExtraction() {
        // temporary outage of spotlight server
        if (DateTime.now().isBefore(DateTime.parse("2015-06-16"))) {
            return;
        }

        KeywordsExtractor extractor = new SpotlightKeywordsExtractor(new SpotlightResourceParser());
        Optional<WikipediaKeyword> maybeKeyword = extractor.tryGuessSingleKeywordFromText("Aluminium Foil");
        assertTrue(maybeKeyword.isPresent());
    }
}
