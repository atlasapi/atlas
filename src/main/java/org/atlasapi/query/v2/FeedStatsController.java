package org.atlasapi.query.v2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.feeds.youview.statistics.FeedStatisticsResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.google.common.base.Strings;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.http.HttpStatusCode;


@Controller
public class FeedStatsController extends BaseController<Iterable<FeedStatistics>> {
    
    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
            .withMessage("No Feed exists for the specified Publisher")
            .withErrorCode("Feed not found")
            .withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
            .withMessage("You require an API key to view this data")
            .withErrorCode("Api Key required")
            .withStatusCode(HttpStatusCode.FORBIDDEN);
    
    private final FeedStatisticsResolver statsResolver;
    
    public FeedStatsController(
            ApplicationFetcher configFetcher,
            AdapterLog log,
            AtlasModelWriter<Iterable<FeedStatistics>> outputter,
            FeedStatisticsResolver statsResolver
    ) {
        super(configFetcher, log, outputter, DefaultApplication.createDefault());
        this.statsResolver = checkNotNull(statsResolver);
    }

    @RequestMapping(value="/3.0/feeds/youview/{publisher}/statistics.json", method = RequestMethod.GET)
    public void statistics(
            HttpServletRequest request,
            HttpServletResponse response,
            @PathVariable("publisher") String publisherStr
    ) throws IOException {
        String isoDuration = request.getParameter("duration");

        if (Strings.isNullOrEmpty(isoDuration)) {
            errorViewFor(request,
                    response,
                    AtlasErrorSummary.forException(new IllegalArgumentException(
                            "The fromTime request parameter must be specified"
                    ))
            );
        }

        java.time.Duration duration = java.time.Duration.parse(isoDuration);

        try {
            Application application;
            try {
                application = application(request);
            } catch (InvalidApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }

            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            if (!application.getConfiguration().isReadEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<FeedStatistics> resolved = statsResolver.resolveFor(publisher, duration);
            if (!resolved.isPresent()) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }

            modelAndViewFor(request, response, ImmutableSet.of(resolved.get()), application);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
}
