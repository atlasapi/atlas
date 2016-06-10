package org.atlasapi.query;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingTimeFilterTest {

    @Mock private HttpServletRequest request;
    @Mock private HttpServletResponse response;
    @Mock private FilterChain filterChain;

    @Mock private Clock clock;

    private ProcessingTimeFilter processingTimeFilter;

    @Before
    public void setUp() throws Exception {
        processingTimeFilter = ProcessingTimeFilter.create(clock);

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        when(clock.getZone()).thenReturn(ZoneOffset.UTC);
        when(clock.instant()).thenReturn(
                Instant.from(now),
                Instant.from(now.plusSeconds(1))
        );
    }

    @Test
    public void doFilterInternal() throws Exception {
        processingTimeFilter.doFilterInternal(request, response, filterChain);

        InOrder order = inOrder(clock, response, filterChain);
        order.verify(clock).instant();
        order.verify(clock).getZone();
        order.verify(filterChain).doFilter(request, response);
        order.verify(clock).instant();
        order.verify(clock).getZone();

        verify(response).addHeader(
                ProcessingTimeFilter.PROCESSING_TIME_HEADER,
                String.valueOf(Duration.ofSeconds(1).toMillis())
        );
    }
}
