package org.atlasapi.system;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.HttpStatusCode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentPurger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletResponse;

import static com.google.common.base.Preconditions.checkNotNull;


@Controller
public class LyrebirdYoutubeContentPurgeController {

    private final ContentPurger contentPurger;
    
    public LyrebirdYoutubeContentPurgeController(ContentPurger contentPurger) {
        this.contentPurger = checkNotNull(contentPurger);
    }
    
    @RequestMapping(value = "/system/content/purge/lyrebird-youtube", method = RequestMethod.POST)
    public void purge(HttpServletResponse response) {
        contentPurger.purge(Publisher.LYREBIRD_YOUTUBE, ImmutableSet.of(Publisher.BBC_LYREBIRD));
        response.setStatus(HttpStatusCode.OK.code());
    }
}
