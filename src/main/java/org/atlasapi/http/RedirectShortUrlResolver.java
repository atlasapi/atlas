package org.atlasapi.http;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.impl.DefaultHttpClientConnection;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HTTP;
import org.atlasapi.remotesite.HttpClients;
import org.joda.time.Duration;

import java.io.IOException;
import java.net.Socket;
import java.net.URL;

public class RedirectShortUrlResolver implements ShortUrlResolver {

	private static final String LOCATION_HEADER = "Location";
	private static final int HTTP_DEFAULT_PORT = 80;
	
	private final Duration timeout = Duration.standardSeconds(4);
	private final BasicHttpParams params;

	
	public RedirectShortUrlResolver() {
		params = new BasicHttpParams();
		HttpConnectionParams.setTcpNoDelay(params, true);
		HttpConnectionParams.setSoTimeout(params, (int) timeout.getMillis());
		HttpConnectionParams.setLinger(params, 0);
	}
	
	@Override
	public String resolve(String shortUri) {
		DefaultHttpClientConnection connection = null;
		
		try {
			URL url = new URL(shortUri);
			
			connection = new DefaultHttpClientConnection();  
			
			connection.bind(new Socket(url.getHost(), HTTP_DEFAULT_PORT), params);
	        	        
			BasicHttpRequest request = new BasicHttpRequest("HEAD", url.getFile());
			request.addHeader(HTTP.TARGET_HOST, url.getHost());
			request.addHeader(HTTP.USER_AGENT,  HttpClients.ATLAS_USER_AGENT);
	        
            connection.sendRequestHeader(request);
            connection.flush();
	        
	        HttpResponse response = connection.receiveResponseHeader();

	        Header location = response.getFirstHeader(LOCATION_HEADER);
	        
	        if (location == null) {
	        	return null;
	        }
	        return location.getValue();
		} 
		catch (Exception e) {
			return null;
		}
		finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					// Not much that can be done
				}
			}
		}
	}
}
