package org.atlasapi.query;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This keeps track of how long it takes to process a request and adds it as a response header.
 * It is intended to give us a metric of how long it takes to process a request that does not
 * include any time waiting for a request thread to become available.
 */
public class ProcessingTimeFilter extends OncePerRequestFilter {

    public static final String PROCESSING_TIME_HEADER = "Processing-Time";

    private final Clock clock;

    public ProcessingTimeFilter() {
        this(Clock.systemUTC());
    }

    private ProcessingTimeFilter(Clock clock) {
        this.clock = checkNotNull(clock);
    }

    @VisibleForTesting
    static ProcessingTimeFilter create(Clock clock) {
        return new ProcessingTimeFilter(clock);
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        ZonedDateTime start = ZonedDateTime.now(clock);

        try {
            filterChain.doFilter(request, response);
        } finally {
            ZonedDateTime end = ZonedDateTime.now(clock);

            response.addHeader(
                    PROCESSING_TIME_HEADER,
                    String.valueOf(
                            Duration.between(start, end).toMillis()
                    )
            );
        }
    }
}
