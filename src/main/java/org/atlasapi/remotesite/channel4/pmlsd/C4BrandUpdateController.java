package org.atlasapi.remotesite.channel4.pmlsd;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.entity.Policy.Platform;

import com.metabroadcast.columbus.telescope.client.ModelWithPayload;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

@Controller
public class C4BrandUpdateController {

    private C4BrandUpdater dflt;
    private ImmutableMap<Platform, C4BrandUpdater> platformSpecific;

    public C4BrandUpdateController(C4BrandUpdater dflt, Map<Platform, ? extends C4BrandUpdater> platformSpecific) {
        this.dflt = dflt;
        this.platformSpecific = ImmutableMap.copyOf(platformSpecific);
    }

    @RequestMapping(value="/system/update/c4",method=RequestMethod.POST)
    public void updateBrand(HttpServletResponse resp, 
            @RequestParam("uri") String brandUri, 
            @RequestParam(value="platform",required=false) String platform) throws IOException {
        
        C4BrandUpdater updater;
        
        if (Strings.isNullOrEmpty(platform)) {
            updater = dflt;
        } else {
            Platform requestedPlatform = Platform.fromKey(platform);
            if (requestedPlatform == null) {
                resp.sendError(400, "Unknown platform " + platform);
                return;
            } else {
                updater = platformSpecific.get(requestedPlatform);
                if (updater == null) {
                    resp.sendError(400, "No updater for platform " + platform);
                    return;
                }
            }
        }
        
        if (!updater.canFetch(brandUri)) {
            resp.sendError(400, "Can't update URI " + brandUri);
            return;
        }
        
        updater.createOrUpdateBrand(new ModelWithPayload<>(brandUri, null));
        
    }
}
