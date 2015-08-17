package org.atlasapi.system;

import com.metabroadcast.common.http.HttpStatusCode;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class PaContentDeactivationController {

    private final PaContentDeactivator paDeactivator;

    public PaContentDeactivationController(PaContentDeactivator paDeactivator) {
        this.paDeactivator = checkNotNull(paDeactivator);
    }

    @RequestMapping(value = "/system/content/deactivate/pa", method = RequestMethod.POST)
    public void purge(HttpServletResponse response, @RequestParam(value = "filePath", required = true) String filePath) throws IOException {
        paDeactivator.deactivate(new File(filePath));
        response.setStatus(HttpStatusCode.OK.code());
        response.flushBuffer();
    }
}
