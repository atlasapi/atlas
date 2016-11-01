package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class C4AtomApiTest {

    private Map<String, String> map = Maps.newConcurrentMap();

    @Test
    public void testConvertsPcOdToBrandIosUri() {

        map.put("dc:relation.programmeId", "338672-002");
        Assert.assertThat(
                C4AtomApi.iOsUriFromPcUri("http://www.channel4.com/programmes/come-dine-with-me/on-demand/338672-002", map),
                is("all4://views/brands?brand=come-dine-with-me&programme=338672-002"));
    }
}
