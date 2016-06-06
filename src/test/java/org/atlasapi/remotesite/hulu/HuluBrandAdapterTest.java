package org.atlasapi.remotesite.hulu;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.SiteSpecificAdapter;
import org.atlasapi.remotesite.hulu.WritingHuluBrandAdapter.HuluBrandCanonicaliser;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class HuluBrandAdapterTest {
    
    @Mock private SiteSpecificAdapter<Episode> episodeAdapter;
    @Mock private ContentWriter contentWriter;

    private WritingHuluBrandAdapter adapter;

    @Before
    public void setUp() throws Exception {
        AdapterLog log = new SystemOutAdapterLog();
        HuluClient client = new HttpBackedHuluClient(HttpClients.webserviceClient(), log);

        adapter = new WritingHuluBrandAdapter(client, episodeAdapter, contentWriter, log);
    }

    @Test
    public void testShouldBeAbleToFetchBrands() throws Exception {
        assertThat(adapter.canFetch("http://www.hulu.com/glee"), is(true));
        assertThat(adapter.canFetch("http://www.hulu.com/watch/123/glee"), is(false));
    }

    @Test
    public void testShouldCanonicalise() throws Exception {
        assertThat(
                new HuluBrandCanonicaliser().canonicalise("http://www.hulu.com/glee"),
                is("http://www.hulu.com/glee")
        );
        assertThat(
                new HuluBrandCanonicaliser().canonicalise("http://www.hulu.com/nfl/americas-game"),
                is("http://www.hulu.com/americas-game")
        );
        assertThat(
                new HuluBrandCanonicaliser().canonicalise("http://www.hulu.com/watch/489427/glee"),
                is(nullValue())
        );
    }
}
