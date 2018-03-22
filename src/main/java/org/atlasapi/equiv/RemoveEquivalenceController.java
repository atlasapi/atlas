package org.atlasapi.equiv;

import com.metabroadcast.common.http.HttpStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class RemoveEquivalenceController {

    private static final Logger log = LoggerFactory.getLogger(RemoveEquivalenceController.class);
    
    private final EquivalenceBreaker equivalenceBreaker;
    
    public RemoveEquivalenceController(EquivalenceBreaker equivalenceBreaker) {
        this.equivalenceBreaker = checkNotNull(equivalenceBreaker);
    }
    
    @RequestMapping(value="/system/equivalence/remove", method=RequestMethod.POST)
    public void removeEquivalence(
            @RequestParam(value="from", required=true) String sourceUri, 
            @RequestParam(value="to", required=true) String targetUriToRemove,
            HttpServletResponse response) throws IOException {
        
        try {
            equivalenceBreaker.removeFromSet(sourceUri, targetUriToRemove);
            response.setStatus(HttpStatusCode.OK.code());
        } catch (Exception e) {
            response.sendError(HttpStatusCode.SERVER_ERROR.code(), String.format("%s", e.getMessage()));
            log.error("Error breaking equivalence from " + sourceUri + " to " + targetUriToRemove, e);
        }
    }
}
