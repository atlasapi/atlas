package org.atlasapi.query.v2;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.time.DateTimeZones.UTC;
import static org.joda.time.DateTimeConstants.JANUARY;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBElement;

import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.tvanytime.TvaGenerationException;
import org.atlasapi.feeds.tvanytime.granular.GranularTvAnytimeGenerator;
import org.atlasapi.feeds.youview.hierarchy.ContentHierarchyExpander;
import org.atlasapi.feeds.youview.hierarchy.ItemAndVersion;
import org.atlasapi.feeds.youview.hierarchy.ItemBroadcastHierarchy;
import org.atlasapi.feeds.youview.hierarchy.ItemOnDemandHierarchy;
import org.atlasapi.feeds.youview.tasks.Destination.DestinationType;
import org.atlasapi.feeds.youview.upload.granular.FilterFactory;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.logging.AdapterLog;
import org.joda.time.DateTime;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import tva.metadata._2010.TVAMainType;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.http.HttpStatusCode;

/**
 * Produces an output feed a certain provider, given a certain publisher's content.
 * <p>
 * Currently, the only feed supported is the TVAnytime output for YouView
 * 
 * @author Oliver Hall (oli@metabroadcast.com)
 *
 */
@Controller
public class ContentFeedController extends BaseController<JAXBElement<TVAMainType>> {

    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
            .withMessage("You require an API key to view this data")
            .withErrorCode("Api Key required")
            .withStatusCode(HttpStatusCode.FORBIDDEN);
    private static final AtlasErrorSummary NO_URI = new AtlasErrorSummary(new NullPointerException())
            .withMessage("Required parameter 'uri' is missing")
            .withErrorCode("Uri parameter required")
            .withStatusCode(HttpStatusCode.BAD_REQUEST);
    private static final AtlasErrorSummary CONTENT_NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
            .withMessage("Unable to resolve content with the provided uri")
            .withErrorCode("Content not found")
            .withStatusCode(HttpStatusCode.BAD_REQUEST);
    private static final AtlasErrorSummary CONTENT_NOT_AN_ITEM = new AtlasErrorSummary(new NullPointerException())
            .withMessage("Resolved Content is not an Item, but needs to be")
            .withErrorCode("Content not an Item")
            .withStatusCode(HttpStatusCode.BAD_REQUEST);
    private static final AtlasErrorSummary ELEMENT_NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
            .withMessage("Unable to resolve element with the provided id")
            .withErrorCode("Element not found")
            .withStatusCode(HttpStatusCode.BAD_REQUEST);
    private static final AtlasErrorSummary INVALID_DESTINATION_TYPE = new AtlasErrorSummary(new NullPointerException())
            .withMessage("No Feed exists of the provided type")
            .withErrorCode("Feed Type not found")
            .withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final DateTime START_OF_TIME = new DateTime(2000, JANUARY, 0, 0, 0, 0, UTC);
    
    private final GranularTvAnytimeGenerator feedGenerator;
    private final ContentResolver contentResolver;
    private final ContentHierarchyExpander hierarchyExpander;
    
    public ContentFeedController(ApplicationConfigurationFetcher configFetcher, AdapterLog log, 
            AtlasModelWriter<JAXBElement<TVAMainType>> outputter, GranularTvAnytimeGenerator feedGenerator, 
            ContentResolver contentResolver, ContentHierarchyExpander hierarchyExpander) {
        super(configFetcher, log, outputter);
        this.feedGenerator = checkNotNull(feedGenerator);
        this.contentResolver = checkNotNull(contentResolver);
        this.hierarchyExpander = checkNotNull(hierarchyExpander);
    }
    
    /**
     * Produces an output feed a certain provider, given a certain publisher's content
     * @param uri -         if present, the endpoint will return the xml generated for that 
     *                      particular item else it will check the lastUpdated parameter.                    
     * @param lastUpdated - if present, the endpoint will return a delta feed of all items 
     *                      updated since lastUpdated, otherwise it will return a full 
     *                      bootstrap feed
     * @throws IOException 
     */
    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}.xml", method = RequestMethod.GET)
    public void generateFeed(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = true) String uri) throws IOException {
        try {
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            ApplicationConfiguration appConfig = appConfig(request);
            DestinationType destinationType = parseDestinationFrom(destinationTypeStr);
            
            if (destinationType == null) {
                errorViewFor(request, response, INVALID_DESTINATION_TYPE);
                return;
            }
            
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<String> possibleUri = Optional.fromNullable(uri);
            if (!possibleUri.isPresent()) {
                errorViewFor(request, response, NO_URI);
                return;
            }
            Optional<Content> content = getContent(publisher, possibleUri.get());
            if (!content.isPresent()) {
                errorViewFor(request, response, CONTENT_NOT_FOUND);
                return;
            }
            if (content.get() instanceof Item) {
                Item item = (Item) content.get();
                // TODO what is the default here?
                Predicate<ItemBroadcastHierarchy> broadcastFilter = FilterFactory.broadcastFilter(START_OF_TIME);
                
                Map<String, ItemAndVersion> versions = hierarchyExpander.versionHierarchiesFor(item);
                Map<String, ItemBroadcastHierarchy> broadcasts = Maps.filterValues(hierarchyExpander.broadcastHierarchiesFor(item), broadcastFilter);
                Map<String, ItemOnDemandHierarchy> onDemands = hierarchyExpander.onDemandHierarchiesFor(item);
                
                JAXBElement<TVAMainType> tva = feedGenerator.generateContentTVAFrom(content.get(), versions, broadcasts, onDemands);
                modelAndViewFor(request, response, tva, appConfig);
            } else {
                JAXBElement<TVAMainType> tva = feedGenerator.generateContentTVAFrom(content.get());
                modelAndViewFor(request, response, tva, appConfig);
            }
            
        } catch (TvaGenerationException e) {
            errorViewFor(request, response, tvaGenerationError(e));
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}/content.xml", method = RequestMethod.GET)
    public void generateContentFeed(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = true) String uri) throws IOException {
        try {
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            ApplicationConfiguration appConfig = appConfig(request);
            DestinationType destinationType = parseDestinationFrom(destinationTypeStr);
            
            if (destinationType == null) {
                errorViewFor(request, response, INVALID_DESTINATION_TYPE);
                return;
            }
            
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<String> possibleUri = Optional.fromNullable(uri);
            if (!possibleUri.isPresent()) {
                errorViewFor(request, response, NO_URI);
                return;
            }
            Optional<Content> content = getContent(publisher, possibleUri.get());
            if (!content.isPresent()) {
                errorViewFor(request, response, CONTENT_NOT_FOUND);
                return;
            }
            JAXBElement<TVAMainType> tva = feedGenerator.generateContentTVAFrom(content.get());
            
            modelAndViewFor(request, response, tva, appConfig);
        } catch (TvaGenerationException e) {
            errorViewFor(request, response, tvaGenerationError(e));
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
    
    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}/versions.xml", method = RequestMethod.GET)
    public void generateVersionsFeed(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = true) String uri,
            @RequestParam(value = "version_crid", required = false) String versionCrid) throws IOException {
        try {
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            ApplicationConfiguration appConfig = appConfig(request);
            DestinationType destinationType = parseDestinationFrom(destinationTypeStr);
            
            if (destinationType == null) {
                errorViewFor(request, response, INVALID_DESTINATION_TYPE);
                return;
            }
            
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<String> possibleUri = Optional.fromNullable(uri);
            if (!possibleUri.isPresent()) {
                errorViewFor(request, response, NO_URI);
                return;
            }
            Optional<Content> content = getContent(publisher, possibleUri.get());
            if (!content.isPresent()) {
                errorViewFor(request, response, CONTENT_NOT_FOUND);
                return;
            }
            if (!(content.get() instanceof Item)) {
                errorViewFor(request, response, CONTENT_NOT_AN_ITEM);
                return;
            }
            Map<String, ItemAndVersion> hierarchies = hierarchyExpander.versionHierarchiesFor((Item) content.get());
            
            if (versionCrid != null) {
                Optional<ItemAndVersion> versionHierarchy = Optional.fromNullable(hierarchies.get(versionCrid));
                if (!versionHierarchy.isPresent()) {
                    errorViewFor(request, response, ELEMENT_NOT_FOUND);
                    return;
                }
                JAXBElement<TVAMainType> tva = feedGenerator.generateVersionTVAFrom(versionHierarchy.get(), versionCrid);
                modelAndViewFor(request, response, tva, appConfig);
            } else {
                JAXBElement<TVAMainType> tva = feedGenerator.generateVersionTVAFrom(hierarchies);
                modelAndViewFor(request, response, tva, appConfig);
            }
        } catch (TvaGenerationException e) {
            errorViewFor(request, response, tvaGenerationError(e));
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
    
    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}/broadcasts.xml", method = RequestMethod.GET)
    public void generateBroadcastFeed(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = true) String uri,
            @RequestParam(value = "broadcast_imi", required = false) String broadcastImi) throws IOException {
        try {
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            ApplicationConfiguration appConfig = appConfig(request);
            DestinationType destinationType = parseDestinationFrom(destinationTypeStr);
            
            if (destinationType == null) {
                errorViewFor(request, response, INVALID_DESTINATION_TYPE);
                return;
            }
            
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<String> possibleUri = Optional.fromNullable(uri);
            if (!possibleUri.isPresent()) {
                errorViewFor(request, response, NO_URI);
                return;
            }
            Optional<Content> content = getContent(publisher, possibleUri.get());
            if (!content.isPresent()) {
                errorViewFor(request, response, CONTENT_NOT_FOUND);
                return;
            }
            if (!(content.get() instanceof Item)) {
                errorViewFor(request, response, CONTENT_NOT_AN_ITEM);
                return;
            }
            
            Map<String, ItemBroadcastHierarchy> hierarchies = hierarchyExpander.broadcastHierarchiesFor((Item) content.get());
            
            if (broadcastImi != null) {
                Optional<ItemBroadcastHierarchy> broadcastHierarchy = Optional.fromNullable(hierarchies.get(broadcastImi));
                if (!broadcastHierarchy.isPresent()) {
                    errorViewFor(request, response, ELEMENT_NOT_FOUND);
                    return;
                }
                JAXBElement<TVAMainType> tva = feedGenerator.generateBroadcastTVAFrom(broadcastHierarchy.get(), broadcastImi);
                modelAndViewFor(request, response, tva, appConfig);
            } else {
                JAXBElement<TVAMainType> tva = feedGenerator.generateBroadcastTVAFrom(hierarchies);
                modelAndViewFor(request, response, tva, appConfig);
            }
        } catch (TvaGenerationException e) {
            errorViewFor(request, response, tvaGenerationError(e));
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }

    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}/on_demands.xml", method = RequestMethod.GET)
    public void generateOnDemandFeed(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = true) String uri,
            @RequestParam(value = "on_demand_imi", required = false) String onDemandImi) throws IOException {
        try {
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            ApplicationConfiguration appConfig = appConfig(request);
            DestinationType destinationType = parseDestinationFrom(destinationTypeStr);
            
            if (destinationType == null) {
                errorViewFor(request, response, INVALID_DESTINATION_TYPE);
                return;
            }
            
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<String> possibleUri = Optional.fromNullable(uri);
            if (!possibleUri.isPresent()) {
                errorViewFor(request, response, NO_URI);
                return;
            }
            Optional<Content> content = getContent(publisher, possibleUri.get());
            if (!content.isPresent()) {
                errorViewFor(request, response, CONTENT_NOT_FOUND);
                return;
            }
            if (!(content.get() instanceof Item)) {
                errorViewFor(request, response, CONTENT_NOT_AN_ITEM);
                return;
            }

            Map<String, ItemOnDemandHierarchy> hierarchies = hierarchyExpander.onDemandHierarchiesFor((Item) content.get());
            
            if (onDemandImi != null) {
                Optional<ItemOnDemandHierarchy> onDemandHierarchy = Optional.fromNullable(hierarchies.get(onDemandImi));
                if (!onDemandHierarchy.isPresent()) {
                    errorViewFor(request, response, ELEMENT_NOT_FOUND);
                    return;
                }
                JAXBElement<TVAMainType> tva = feedGenerator.generateOnDemandTVAFrom(onDemandHierarchy.get(), onDemandImi);
                modelAndViewFor(request, response, tva, appConfig);
            } else {
                JAXBElement<TVAMainType> tva = feedGenerator.generateOnDemandTVAFrom(hierarchies);
                modelAndViewFor(request, response, tva, appConfig);
            }
        } catch (TvaGenerationException e) {
            errorViewFor(request, response, tvaGenerationError(e));
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
    
    private DestinationType parseDestinationFrom(String destinationTypeStr) {
        for (DestinationType destinationType : DestinationType.values()) {
            if (destinationType.name().equalsIgnoreCase(destinationTypeStr)) {
                return destinationType;
            }
        }
        return null;
    }

    private AtlasErrorSummary tvaGenerationError(TvaGenerationException e) { 
        return new AtlasErrorSummary(e)
                .withMessage("Unable to generate TVAnytime output for the provided uri")
                .withErrorCode("TVAnytime generation error")
                .withStatusCode(HttpStatusCode.SERVER_ERROR);
    }
    
    private Optional<Content> getContent(Publisher publisher, String uri) {
        ResolvedContent resolvedContent = contentResolver.findByCanonicalUris(ImmutableList.of(uri));
        Collection<Identified> resolved = resolvedContent.asResolvedMap().values();
        return Optional.fromNullable((Content) Iterables.getOnlyElement(resolved, null));
    }
}
