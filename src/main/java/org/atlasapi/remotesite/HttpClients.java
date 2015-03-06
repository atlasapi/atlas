package org.atlasapi.remotesite;

import java.net.URL;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;

import org.apache.http.conn.ssl.SSLSocketFactory;

import com.metabroadcast.common.http.OAuthSimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.media.MimeType;
import java.io.FileInputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import org.apache.http.conn.ssl.SSLSocketFactory;

public class HttpClients {

    public static final String ATLAS_USER_AGENT = "Mozilla/5.0 (compatible; atlas/3.0; +http://atlas.metabroadcast.com)";

    /**
     * @return A {@link SimpleHttpClient} that is configured to work
     * well with 3rd party web services that are expected to return data
     * reasonably quickly.
     */
    public static SimpleHttpClient webserviceClient() {
        return new SimpleHttpClientBuilder()
            .withUserAgent(ATLAS_USER_AGENT)
            .withSocketTimeout(30, TimeUnit.SECONDS)
            .withRetries(3)
        .build();
    }

	public static SimpleHttpClient screenScrapingClient() {
		return new SimpleHttpClientBuilder()
	        .withUserAgent(ATLAS_USER_AGENT)
	        .withSocketTimeout(50, TimeUnit.SECONDS)
	        .withConnectionTimeout(10, TimeUnit.SECONDS)
	        .withAcceptHeader(MimeType.TEXT_HTML)
	        .withRetries(3)
        .build();
	}

    public static SimpleHttpClient oauthClient(String apiKey, String apiSecret) {
        return new OAuthSimpleHttpClient.Builder()
                .withApiKey(apiKey)
                .withApiSecret(apiSecret)
                .withConnectTimeout(10, TimeUnit.SECONDS)
                .withReadTimeout(10, TimeUnit.SECONDS)
                .withAcceptContentType("application/xml")
                .withAcceptCharset("utf-8")
                .withUserAgent(ATLAS_USER_AGENT)
                .build();
    }
    
    public static SimpleHttpClient httpsClient(URL keystore, String password) throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS", "SUN");
        ks.load(keystore.openStream(), password.toCharArray());
        return new SimpleHttpClientBuilder()
            .withUserAgent(ATLAS_USER_AGENT)
            .withSocketTimeout(30, TimeUnit.SECONDS)
            .withSslSocketFactory(new SSLSocketFactory(ks, password, ks))
            .build();
    }

    public static SimpleHttpClient httpsClientThatDoesntCheckCerts(URL keystore, String password) throws Exception {
        KeyStore ks = KeyStore.getInstance("JKS", "SUN");
        ks.load(keystore.openStream(), password.toCharArray());
        return new SimpleHttpClientBuilder()
                .withUserAgent(ATLAS_USER_AGENT)
                .withSocketTimeout(30, TimeUnit.SECONDS)
                .withSslSocketFactory(new SSLSocketFactory(ks, password, ks))
                .withTrustUnverifiedCerts()
                .build();
    }
}