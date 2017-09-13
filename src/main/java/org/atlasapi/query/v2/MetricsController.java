package org.atlasapi.query.v2;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class MetricsController extends MetricsServlet {

    private MetricsController(CollectorRegistry registry) {
        super(registry);
    }

    public static MetricsController create(CollectorRegistry registry) {
        return new MetricsController(registry);
    }

    @RequestMapping(value = "/system/prometheusMetrics", method = RequestMethod.GET)
    public void getPrometheusMetrics(
            HttpServletRequest req,
            HttpServletResponse resp
    ) throws IOException, ServletException {
        doGet(req, resp);
    }
}
