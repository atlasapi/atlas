package org.atlasapi.system.Health;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metabroadcast.common.health.Health;
import com.metabroadcast.common.health.Result;
import com.metabroadcast.common.health.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

@Controller
public class HealthController {

    private static final Logger log = LoggerFactory.getLogger(HealthController.class);

    private final Health health;
    private final ObjectMapper mapper;

    private HealthController(Health health, ObjectMapper mapper) {
        this.health = health;
        this.mapper = mapper;
    }


    @RequestMapping("/system/health/alive")
    public void isAlive(HttpServletResponse response) {
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @RequestMapping("/system/health/probes")
    public void checkMongoHealth(HttpServletResponse response) throws IOException {
        Result result = health.status(Health.FailurePolicy.ANY);
        response.setContentType("application/json");
        ServletOutputStream out = response.getOutputStream();

        if (Status.UNHEALTHY.equals(result.getStatus())) {
            result.getProbeResults().stream()
                    .filter(probeResult -> probeResult.getStatus() == Status.UNHEALTHY)
                    .forEach(probeResult -> {
                        try {
                            mapper.writeValue(out, probeResult);
                        } catch(IOException e) {
                            log.error("Could not write unhealthy probe result: {}",
                                    probeResult.getMsg().orElse("no msg"),
                                    e
                            );
                        }
                    });
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    @RequestMapping("/system/threads")
    public void showThreads(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
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
}
