package org.atlasapi.remotesite.tvblob;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TVBlobDayAdapterTest extends TestCase {

    @Mock private ContentResolver resolver;
    @Mock private ContentWriter writer;

    private TVBlobDayAdapter adapter;

    @Before
    public void setUp() throws Exception {
        adapter = new TVBlobDayAdapter(writer, resolver);
    }

    @Test
    public void testCanFetch() {
        assertThat(
                adapter.canPopulate(
                        "http://epgadmin.tvblob.com/api/raiuno/programmes/schedules/today.json"
                ),
                is(true)
        );
    }
}
