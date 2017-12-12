package org.atlasapi.equiv;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.HttpStatusCode;
import org.atlasapi.media.entity.Container;
import org.atlasapi.persistence.content.ContentResolver;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Controller
public class ChildRefUpdateController {

    private final ChildRefUpdateTask updater;
    private final ContentResolver resolver;
    private final ExecutorService executor;

    public ChildRefUpdateController(ChildRefUpdateTask updater, ContentResolver resolver) {
        this.updater = updater;
        this.resolver = resolver;
        this.executor = Executors.newSingleThreadExecutor();
    }
    
    @RequestMapping(value="/system/update/childrefs",method=RequestMethod.POST)
    public void updateChildRefsForContainer(HttpServletResponse response, final @RequestParam("uri") String uri) throws IOException {
        
        Future<?> submit = executor.submit(new Runnable() {
            @Override
            public void run() {
                updater.updateContainerReferences((Container) resolver.findByCanonicalUris(ImmutableSet.of(uri)).get(uri).requireValue());
            }
        });
        
        try {
            submit.get();
            response.setStatus(HttpStatusCode.OK.code());
        } catch (Exception e) {
            response.sendError(HttpStatusCode.SERVER_ERROR.code(), String.format("%s: %s", uri, e.getMessage()));
        }
        
    }
}
