package org.atlasapi.system;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import org.atlasapi.AtlasMain;
import org.springframework.web.context.ServletContextAware;

import javax.servlet.ServletContext;
import java.util.Map;
import java.util.Map.Entry;


public class JettyHealthProbe implements HealthProbe, ServletContextAware {

    private ServletContext servletContext;

    @Override
    public ProbeResult probe() throws Exception {
        ProbeResult probeResult = new ProbeResult("Requests");
        Map<String, String> metrics = AtlasMain.getMetrics(
                servletContext.getAttribute(AtlasMain.CONTEXT_ATTRIBUTE));
        
        for (Entry<String, String> entry : metrics.entrySet()) {
            probeResult.addInfo(entry.getKey(), entry.getValue());
        }
        return probeResult;
    }

    @Override
    public String title() {
        return "Jetty";
    }

    @Override
    public String slug() {
        return "jetty";
    }

    @Override
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

}
