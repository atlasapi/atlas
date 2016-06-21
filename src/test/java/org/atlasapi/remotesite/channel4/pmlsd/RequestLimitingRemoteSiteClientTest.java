package org.atlasapi.remotesite.channel4.pmlsd;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.atlasapi.persistence.system.RemoteSiteClient;

public class RequestLimitingRemoteSiteClientTest extends TestCase {

	public void testThatRequestsAreLimited() throws Exception {
		
		final AtomicInteger delegatedRequests = new AtomicInteger(0);
		
		RemoteSiteClient<Void> countingDelegate = new RemoteSiteClient<Void>() {
			@Override
			public Void get(String uri) throws Exception {
				delegatedRequests.incrementAndGet();
				return null;
			}
		};
		
		RequestLimitingRemoteSiteClient<Void> client = new RequestLimitingRemoteSiteClient<Void>(countingDelegate, 2);

		long timeBefore = System.currentTimeMillis();
		
		for (int i = 0; i < 4; i++) {
			client.get(null);
		}
		
		long duration = System.currentTimeMillis() - timeBefore;
		
		assertEquals(4, delegatedRequests.get());
		
		assertTrue(duration >= 1000 && duration <= 3000);
	}
}
