package org.atlasapi.query.v2;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationFetcher;
import org.atlasapi.application.query.InvalidApiKeyException;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.feeds.youview.statistics.FeedStatistics;
import org.atlasapi.feeds.youview.statistics.FeedStatisticsResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.http.HttpHeaders;
import com.metabroadcast.common.http.HttpStatusCode;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Flushables;
import org.joda.time.Period;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;

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
    public void statistics(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("publisher") String publisherStr, @RequestParam("timespan") String timespan
    ) throws IOException {
        // we parse the ISO 8601 duration as a period because hours are inexact in terms of milliseconds
        Period timeBeforeNow = Period.parse(timespan);
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

            Optional<FeedStatistics> resolved = statsResolver.resolveFor(publisher, timeBeforeNow);
            if (!resolved.isPresent()) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }

            modelAndViewFor(request, response, ImmutableSet.of(resolved.get()), application);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @RequestMapping(value="/3.0/feeds/youview/{publisher}/statistics.prometheus", method = RequestMethod.GET)
    public void statisticsProm(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("publisher") String publisherStr, @RequestParam("timespan") String timespan
    ) throws IOException {
        // we parse the ISO 8601 duration as a period because hours are inexact in terms of milliseconds
        Period timeBeforeNow = Period.parse(timespan);
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

            Optional<FeedStatistics> stats = statsResolver.resolveFor(publisher, timeBeforeNow);
            if (!stats.isPresent()) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }

            OutputStream out = response.getOutputStream();
            String accepts = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
            if (accepts != null && accepts.contains("gzip")) {
                response.setHeader(HttpHeaders.CONTENT_ENCODING, "gzip");
                out = new GZIPOutputStream(out);
            }

            OutputStreamWriter writer = new OutputStreamWriter(out, Charsets.UTF_8);

            try {
                String name = "YouViewSuccessfulTasks_" + publisherStr + "_" + timespan;
                writer.write("# TYPE " + name + " gauge\n");
                writer.write(name + " " + stats.get().successfulTasks());
            } finally {
                Flushables.flushQuietly(writer);
                if (out instanceof GZIPOutputStream) {
                    ((GZIPOutputStream) out).finish();
                }
            }


            modelAndViewFor(request, response, ImmutableSet.of(stats.get()), application);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
}
