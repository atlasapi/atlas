package org.atlasapi.system.health;

import com.google.common.collect.Maps;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.Result;
import com.metabroadcast.common.health.Status;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;


@Controller
public class K8HealthController {

    private Map<String, Health> healthMap;

    private K8HealthController() {
        healthMap = Maps.newConcurrentMap();
    }

    public static K8HealthController create() {
        return new K8HealthController();
    }

    public void registerHealth(String name, Health health) {
        healthMap.put(name, health);
    }

    @RequestMapping("/system/health/alive")
    public void isAlive(HttpServletResponse response) {
        response.setStatus(SC_OK);
    }

    @RequestMapping("/system/health/list")
    public void listProbes(HttpServletResponse response) throws IOException {
        response.setStatus(SC_OK);
        response.getWriter().print(healthMap.keySet());
    }

    @RequestMapping("/system/health/probes")
    public void showHealthForProbes(HttpServletResponse response) throws IOException {
        response.setStatus(SC_OK);

        healthMap.entrySet().forEach(entry -> {
            Result result = entry.getValue().status(Health.FailurePolicy.ANY);
            if (result.getStatus() == Status.UNHEALTHY) {
                response.setStatus(SC_INTERNAL_SERVER_ERROR);
            }
        });
    }

    @RequestMapping("/system/health/probes/{slug}")
    public void showHealthForProbe(
            HttpServletResponse response,
            @PathVariable("slug") String slug
    ) throws IOException {

        Optional<Health> health = Optional.ofNullable(healthMap.get(slug));

        if (health.isPresent()) {
            Result result = health.get().status(Health.FailurePolicy.ANY);

            response.setStatus(
                    result.getStatus() == Status.HEALTHY ? SC_OK
                                                         : SC_INTERNAL_SERVER_ERROR
            );
        } else {
            response.setStatus(SC_NOT_FOUND);
            response.getWriter().println("Could not find probe: " + slug);
        }

    }

    @RequestMapping("/system/info/threads")
    public void showThreads(HttpServletResponse response) throws IOException {
        response.setStatus(SC_OK);
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();

        response.getWriter().print(traces.entrySet());

    }
}
