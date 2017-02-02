package org.atlasapi.query.v2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.feeds.youview.statistics.FeedStatisticsResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;
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
    
    public FeedStatsController(ApplicationConfigurationFetcher configFetcher, AdapterLog log,
            AtlasModelWriter<Iterable<FeedStatistics>> outputter, FeedStatisticsResolver statsResolver) {
        super(configFetcher, log, outputter);
        this.statsResolver = checkNotNull(statsResolver);
    }

    @RequestMapping(value="/3.0/feeds/youview/{publisher}/statistics.json", method = RequestMethod.GET)
    public void statistics(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("publisher") String publisherStr) throws IOException {
        try {
            ApplicationConfiguration appConfig;
            try {
                appConfig = appConfig(request);
            } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
                errorViewFor(request, response, AtlasErrorSummary.forException(ex));
                return;
            }

            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<FeedStatistics> resolved = statsResolver.resolveFor(publisher);
            if (!resolved.isPresent()) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }

            modelAndViewFor(request, response, ImmutableSet.of(resolved.get()), appConfig);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
}
