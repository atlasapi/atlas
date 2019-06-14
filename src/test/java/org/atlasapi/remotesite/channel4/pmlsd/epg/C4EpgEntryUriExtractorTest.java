package org.atlasapi.remotesite.channel4.pmlsd.epg;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.remotesite.channel4.pmlsd.C4UriExtractor.C4UriAndAliases;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.C4EpgEntry;
import org.atlasapi.remotesite.channel4.pmlsd.epg.model.TypedLink;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

public class C4EpgEntryUriExtractorTest {

    private final C4EpgEntryUriExtractor extractor = new C4EpgEntryUriExtractor();
    
    @Test
    public void testExtractsBrandFromFullEpisodeAtomUri() {
        extractBrand("http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1/episode-3.atom", 
            "http://pmlsc.channel4.com/pmlsd/the-hoobs");
    }

    @Test
    public void testExtractsBrandFromSeriesAtomUri() {
        extractBrand("http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1.atom", 
                "http://pmlsc.channel4.com/pmlsd/the-hoobs");
    }
    
    @Test
    public void testExtractsBrandFromBrandOnlyAtomUri() {
        extractBrand("http://pmlsc.channel4.com/pmlsd/the-hoobs.atom", 
                "http://pmlsc.channel4.com/pmlsd/the-hoobs");
    }
    
    private void extractBrand(String input, String output) {
        assertThat(
                extractor.uriForBrand(Publisher.C4_PMLSD, entryWithRelatedLink(input)),
                is(Optional.of(C4UriAndAliases.create(output)))
        );
    }

    @Test
    public void testExtractsSeriesFromFullEpisodeAtomUri() {
        C4EpgEntry entry = entryWithRelatedLink("http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1/episode-3.atom");
        extractSeries(entry, "http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1");
    }
    
    @Test
    public void testExtractsSeriesFromSeriesAtomUri() {
        C4EpgEntry entry = entryWithRelatedLink("http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1.atom");
        extractSeries(entry, "http://pmlsc.channel4.com/pmlsd/the-hoobs/episode-guide/series-1");
    }
    
    @Test
    public void testDoesntResolveSeriesFromBrandOnlyAtomUriWhenSeriesNumberIsAbsent() {
        assertFalse(extractor.uriForSeries(Publisher.C4_PMLSD, entryWithRelatedLink("http://pmlsc.channel4.com/pmlsd/the-hoobs.atom")).isPresent());
    }
    
    private void extractSeries(C4EpgEntry input, String output) {
        assertThat(
                extractor.uriForSeries(Publisher.C4_PMLSD, input),
                is(Optional.of(C4UriAndAliases.create(output)))
        );
    }
    
    @Test
    public void testExtractsItemId() {
        C4EpgEntry entry = entryWithoutRelatedLink("tag:pmlsc.channel4.com,2009:slot/26424438")
                .withProgrammeId("40635/014")
                .withTitle("The Treacle People");
        assertThat(
                extractor.uriForItem(Publisher.C4_PMLSD, entry),
                is(Optional.of(C4UriAndAliases.create(
                        "http://pmlsc.channel4.com/pmlsd/40635/014",
                        new Alias(C4AtomApi.ALIAS, "40635/014"),
                        new Alias(C4AtomApi.ALIAS_FOR_BARB, "40635/014")
                )))
        );
    }

    private C4EpgEntry entryWithRelatedLink(String uri) {
        return entryWithIdAndRelatedLink("noid", uri);
    }
    
    private C4EpgEntry entryWithIdAndRelatedLink(String id, String uri) {
        return new C4EpgEntry(id).withLinks(ImmutableList.of(new TypedLink(uri, "related")));
    }

    private C4EpgEntry entryWithoutRelatedLink(String id) {
        return new C4EpgEntry(id);
    }

}
