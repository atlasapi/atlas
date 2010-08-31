package org.atlasapi.remotesite.tvblob;

import java.util.ArrayList;
import java.util.List;

import org.atlasapi.persistence.system.RemoteSiteClient;
import org.atlasapi.remotesite.HttpClients;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.inject.internal.Lists;
import com.metabroadcast.common.http.SimpleHttpClient;

public class TVBlobServicesClient implements RemoteSiteClient<List<TVBlobService>>{

    private ObjectMapper mapper = new ObjectMapper();
    private SimpleHttpClient client = HttpClients.webserviceClient();

    @SuppressWarnings("unchecked")
    @Override
    public List<TVBlobService> get(String url) throws Exception {
        List<TVBlobService> services = Lists.newArrayList();
        
        List<Object> objects =  mapper.readValue(client.getContentsOf(url), ArrayList.class);
        for (Object object: objects) {
            TVBlobService service = mapper.convertValue(object, TVBlobService.class);
            services.add(service);
        }
        
        return services;
    }
}
