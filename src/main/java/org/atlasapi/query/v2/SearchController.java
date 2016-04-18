package org.atlasapi.query.v2;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApiKeyNotFoundException;
import org.atlasapi.application.query.InvalidIpForApiKeyException;
import org.atlasapi.application.query.IpCheckingApiKeyConfigurationFetcher;
import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.query.RevokedApiKeyException;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.output.JsonTranslator;
import org.atlasapi.output.QueryResult;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.search.model.SearchQuery;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.text.MoreStrings;
import org.atlasapi.media.entity.Specialization;

@Controller
public class SearchController extends BaseController<QueryResult<Identified,?extends Identified>> {

    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
            .withStatusCode(HttpStatusCode.FORBIDDEN)
            .withMessage("Your API key is not permitted to view content from this publisher");

    private static final String QUERY_PARAM = "q";
    private static final String SPECIALIZATION_PARAM = "specialization";
    private static final String PUBLISHER_PARAM = "publisher";
    private static final String TITLE_WEIGHTING_PARAM = "titleWeighting";
    private static final String BROADCAST_WEIGHTING_PARAM = "broadcastWeighting";
    private static final String CATCHUP_WEIGHTING_PARAM = "catchupWeighting";
    private static final String TYPE_PARAM = "type";
    private static final String TOP_LEVEL_PARAM = "topLevelOnly";
    private static final String CURRENT_BROADCASTS_ONLY = "currentBroadcastsOnly";
    private static final String PRIORITY_CHANNEL_WEIGHTING = "priorityChannelWeighting";
    private static final String ANNOTATIONS_PARAM = "annotations";

    private static final float DEFAULT_TITLE_WEIGHTING = 1.0f;
    private static final float DEFAULT_PRIORITY_CHANNEL_WEIGHTING = 1.0f;
    private static final float DEFAULT_BROADCAST_WEIGHTING = 0.2f;
    private static final float DEFAULT_CATCHUP_WEIGHTING = 0.15f;

    private final SearchResolver searcher;
    private final ParameterChecker paramChecker = new ParameterChecker(ImmutableSet.of(
        IpCheckingApiKeyConfigurationFetcher.API_KEY_QUERY_PARAMETER,
        Selection.LIMIT_REQUEST_PARAM,
        Selection.START_INDEX_REQUEST_PARAM,
        QUERY_PARAM,
        SPECIALIZATION_PARAM,
        PUBLISHER_PARAM,
        TITLE_WEIGHTING_PARAM,
        BROADCAST_WEIGHTING_PARAM,
        CATCHUP_WEIGHTING_PARAM,
        JsonTranslator.CALLBACK,
        ANNOTATIONS_PARAM,
        TYPE_PARAM,
        TOP_LEVEL_PARAM,
        CURRENT_BROADCASTS_ONLY,
        PRIORITY_CHANNEL_WEIGHTING
    ));
    public SearchController(SearchResolver searcher, ApplicationConfigurationFetcher configFetcher, AdapterLog log, AtlasModelWriter<QueryResult<Identified,?extends Identified>> outputter) {
        super(configFetcher, log, outputter);
        this.searcher = searcher;
    }

    @RequestMapping("/3.0/search.*")
    public void search(@RequestParam(QUERY_PARAM) String q, 
            @RequestParam(value = SPECIALIZATION_PARAM, required = false) String specialization,
            @RequestParam(value = PUBLISHER_PARAM, required = false) String publisher,
            @RequestParam(value = TITLE_WEIGHTING_PARAM, required = false) String titleWeightingParam,
            @RequestParam(value = BROADCAST_WEIGHTING_PARAM, required = false) String broadcastWeightingParam,
            @RequestParam(value = CATCHUP_WEIGHTING_PARAM, required = false) String catchupWeightingParam,
            @RequestParam(value = TYPE_PARAM, required = false) String type, 
            @RequestParam(value = TOP_LEVEL_PARAM, required = false, defaultValue = "true") String topLevel,
            @RequestParam(value = CURRENT_BROADCASTS_ONLY, required = false, defaultValue = "false") String currentBroadcastsOnly,
            @RequestParam(value = PRIORITY_CHANNEL_WEIGHTING, required = false) String priorityChannelWeightingParam,
            HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            paramChecker.checkParameters(request);

            if (Strings.isNullOrEmpty(q)) {
                throw new IllegalArgumentException("You must specify a query parameter");
            }

            Selection selection = Selection.builder().build(request);
            if (!selection.hasLimit()) {
                throw new IllegalArgumentException("You must specify a limit parameter");
            }

            float titleWeighting = getFloatParam(titleWeightingParam, DEFAULT_TITLE_WEIGHTING);
            float broadcastWeighting = getFloatParam(broadcastWeightingParam, DEFAULT_BROADCAST_WEIGHTING);
            float catchupWeighting = getFloatParam(catchupWeightingParam, DEFAULT_CATCHUP_WEIGHTING);
            float priorityChannelWeighting = getFloatParam(priorityChannelWeightingParam, DEFAULT_PRIORITY_CHANNEL_WEIGHTING);

            ApplicationConfiguration appConfig;
            try {
                appConfig = appConfig(request);
            } catch (ApiKeyNotFoundException | RevokedApiKeyException | InvalidIpForApiKeyException ex) {
                outputter.writeError(request, response, FORBIDDEN);
                return;
            }

            Set<Specialization> specializations = specializations(specialization);
            Set<Publisher> publishers = publishers(publisher, appConfig);
            List<Identified> content = searcher.search(SearchQuery.builder(q)
                .withSelection(selection)
                .withSpecializations(specializations)
                .withPublishers(publishers)
                .withTitleWeighting(titleWeighting)
                .withBroadcastWeighting(broadcastWeighting)
                .withCatchupWeighting(catchupWeighting)
                .withPriorityChannelWeighting(priorityChannelWeighting)
                .withType(type)
                .isTopLevelOnly(!Strings.isNullOrEmpty(topLevel) ? Boolean.valueOf(topLevel) : null)
                .withCurrentBroadcastsOnly(!Strings.isNullOrEmpty(currentBroadcastsOnly) ? Boolean.valueOf(currentBroadcastsOnly) : null)
                .build(), appConfig);

            modelAndViewFor(request, response, QueryResult.of(content), appConfig);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    private float getFloatParam(String stringValue, float defaultValue) {
        if (!Strings.isNullOrEmpty(stringValue)) {
            if (MoreStrings.containsOnlyDecimalCharacters(stringValue)) {
                return Float.parseFloat(stringValue);
            }
        }
        return defaultValue;
    }
}
