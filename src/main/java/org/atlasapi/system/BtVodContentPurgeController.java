package org.atlasapi.system;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentPurger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.HttpStatusCode;


@Controller
public class BtVodContentPurgeController {
    
    private final ContentPurger contentPurger;
    
    public BtVodContentPurgeController(ContentPurger contentPurger) {
        this.contentPurger = checkNotNull(contentPurger);
    }
    
    @RequestMapping(value = "/system/content/purge/btvod", method = RequestMethod.POST)
    public void purge(HttpServletResponse response) {
        contentPurger.purge(Publisher.BT_VOD, ImmutableSet.<Publisher>of());
        response.setStatus(HttpStatusCode.OK.code());
    }
    
    @RequestMapping(value = "/system/content/purge/bttvevod", method = RequestMethod.POST)
    public void purgeTve(HttpServletResponse response) {
        contentPurger.purge(Publisher.BT_TVE_VOD, ImmutableSet.<Publisher>of());
        response.setStatus(HttpStatusCode.OK.code());
    }
}
