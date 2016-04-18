package org.atlasapi.remotesite.hulu;

import java.util.concurrent.ExecutorService;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.atlasapi.remotesite.HttpClients;
import org.atlasapi.remotesite.SiteSpecificAdapter;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.util.concurrent.MoreExecutors;

@Ignore
@SuppressWarnings("unchecked")
@RunWith(JMock.class)
public class HuluAllBrandsAdapterTest extends TestCase {
    
    private final Mockery context = new Mockery();
	private final SiteSpecificAdapter<Brand> brandAdapter = context.mock(SiteSpecificAdapter.class);
    private final ExecutorService executor = MoreExecutors.sameThreadExecutor();
    private final AdapterLog log = new NullAdapterLog();
    private final HuluAllBrandsUpdater updater = new HuluAllBrandsUpdater(new HttpBackedHuluClient(HttpClients.webserviceClient(),log), brandAdapter, executor, log);

    @Test
    public void testShouldGetBrand() throws Exception {
    	
        context.checking(new Expectations() {{
            allowing(brandAdapter).canFetch(with(any(String.class))); will(returnValue(true));
            atLeast(100).of(brandAdapter).fetch(with(any(String.class)));
        }});

        updater.run();
    }
}
