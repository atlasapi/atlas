package org.atlasapi.system;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Publisher;
import org.springframework.stereotype.Controller;
import org.atlasapi.persistence.content.ContentPurger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.HttpStatusCode;


@Controller
public class ScrubbablesProducerContentPurgeController {

 private final ContentPurger contentPurger;
    
    public ScrubbablesProducerContentPurgeController(ContentPurger contentPurger) {
        this.contentPurger = checkNotNull(contentPurger);
    }
    
    @RequestMapping(value = "/system/content/purge/scrubbables-producer", method = RequestMethod.POST)
    public void purge(HttpServletResponse response) {
        contentPurger.purge(Publisher.SCRUBBABLES_PRODUCER, ImmutableSet.<Publisher>of());
        response.setStatus(HttpStatusCode.OK.code());
    }
}
