package org.atlasapi.system.Health;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.Result;
import com.metabroadcast.common.health.Status;
import com.metabroadcast.common.health.probes.ProbeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;


@Controller
public class HealthController {

    private static final Logger log = LoggerFactory.getLogger(HealthController.class);
    private static final String JSON_TYPE = "application/json";

    private final ObjectMapper mapper;

    private Map<String, Health> healthMap;

    private HealthController(ObjectMapper mapper) {
        healthMap = Maps.newConcurrentMap();
        this.mapper = mapper;
    }

    public static HealthController create(ObjectMapper mapper) {
        return new HealthController(mapper);
    }

    public void registerHealth(String name, Health health) {
        healthMap.put(name, health);
    }

    @RequestMapping("/system/health/alive")
    public void isAlive(HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @RequestMapping("/system/health/list")
    public void listProbes(HttpServletResponse response) throws IOException {
        response.setStatus(SC_OK);
        response.setContentType(JSON_TYPE);
        OutputStream out = response.getOutputStream();
        mapper.writeValue(out, healthMap.keySet());
        out.close();
    }

    @RequestMapping("/system/health/probes")
    public void showHealthForProbes(HttpServletResponse response) throws IOException {

        ServletOutputStream out = response.getOutputStream();
        response.setContentType(JSON_TYPE);

        healthMap.entrySet().forEach(entry -> {
            Result result = entry.getValue().status(Health.FailurePolicy.ANY);
            response.setStatus(result.getStatus() == Status.HEALTHY ? SC_OK : SC_INTERNAL_SERVER_ERROR);
            result.getProbeResults().forEach(probeResult -> writeResult(out, probeResult));
        });

        out.close();
    }

    @RequestMapping("/system/health/probes/{slug}")
    public void showHealthForProbe(
            HttpServletResponse response,
            @PathVariable("slug") String slug
    ) throws IOException {

        Optional<Health> health = Optional.ofNullable(healthMap.get(slug));
        response.setContentType(JSON_TYPE);

        if (health.isPresent()) {
            Result result = health.get().status(Health.FailurePolicy.ANY);

            response.setStatus(result.getStatus() == Status.HEALTHY ? SC_OK : SC_INTERNAL_SERVER_ERROR);

            ServletOutputStream out = response.getOutputStream();
            result.getProbeResults().forEach(probeResult -> writeResult(out, probeResult));

            out.close();
        } else {
            response.setStatus(SC_NOT_FOUND);
        }

    }

    @RequestMapping("/system/info/threads")
    public void showThreads(HttpServletResponse response) throws IOException {
        response.setContentType(JSON_TYPE);
        response.setStatus(200);
        ServletOutputStream out = response.getOutputStream();
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();

        traces.entrySet().forEach(threadEntry -> {
                try {
                    mapper.writeValue(out, threadEntry);
                } catch (IOException e) {
                    log.error("Could not write trace for thread: {}",
                            threadEntry.getKey().getName(),
                            e
                    );
                }
        });

        out.close();
    }

    private void writeResult(OutputStream out, ProbeResult probeResult) {
        try {
            mapper.writeValue(out, probeResult);
        } catch (IOException e) {
            log.error("Could not write unhealthy probe result: {}",
                    probeResult.getMsg().orElse("no message"),
                    e
            );
        }
    }
}
