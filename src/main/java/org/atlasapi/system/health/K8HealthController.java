package org.atlasapi.system.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.Result;
import com.metabroadcast.common.health.Status;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;


@Controller
public class K8HealthController {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
    private Health health;

    private K8HealthController(Health health) {
        this.health = health;
    }

    public static K8HealthController create(Health health) {
        return new K8HealthController(health);
    }

    @RequestMapping("/system/health/alive")
    public void isAlive(HttpServletResponse response) {
        response.setStatus(SC_OK);
    }

    @RequestMapping("/system/health/probes")
    public void showHealthForProbes(HttpServletResponse response) throws IOException {

        Result result = health.status(Health.FailurePolicy.ALL);

        response.setStatus(
                result.getStatus() == Status.HEALTHY ? SC_OK
                                                     : SC_INTERNAL_SERVER_ERROR
        );

        MAPPER.writeValue(response.getWriter(), result.getProbeResults());
    }

    @RequestMapping("/system/info/threads")
    public void showThreads(HttpServletResponse response) throws IOException {
        response.setStatus(SC_OK);
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();

        MAPPER.writeValue(response.getWriter(), traces);
    }
}
