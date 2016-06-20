package org.atlasapi.remotesite.channel4.pmlsd;

import org.atlasapi.persistence.system.RemoteSiteClient;

/**
 * Very simple rate limiting for 3rd party APIs
 * The concurrency could be improved, however since the limits are often
 * very low (only a few requests per second) it won't make much difference. 
 */
public class RequestLimitingRemoteSiteClient<T> implements RemoteSiteClient<T> {

	private final int waitTimeMs;

	private long lastRequest = 0;

	private final RemoteSiteClient<T> delegate;

	
	public RequestLimitingRemoteSiteClient(RemoteSiteClient<T> delegate, int requestsPerSecond) {
		this.delegate = delegate;
		this.waitTimeMs = (int) (Math.ceil(1000.0 / requestsPerSecond));
	}
	
	@Override
	public  T get(String uri) throws Exception {
		
		synchronized (this) {
			while (true) {
				long now = System.currentTimeMillis();
				long timePassedSinceLastRequest = now - lastRequest;

				if (timePassedSinceLastRequest < waitTimeMs) {
					wait(waitTimeMs - timePassedSinceLastRequest);
				} else {
					break;
				}
			}
			lastRequest = System.currentTimeMillis();
		}
		return delegate.get(uri);
	}
}
