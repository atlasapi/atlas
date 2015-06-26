package org.atlasapi.query.v2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.youview.tasks.Action;
import org.atlasapi.feeds.youview.tasks.Status;
import org.atlasapi.feeds.youview.tasks.TVAElementType;
import org.atlasapi.feeds.youview.tasks.Task;
import org.atlasapi.feeds.youview.tasks.TaskQuery;
import org.atlasapi.feeds.youview.tasks.persistence.TaskStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

@Controller
public class TaskController extends BaseController<Iterable<Task>> {
    
    private static final String NITRO_URI_PREFIX = "http://nitro.bbc.co.uk/programmes/";
    
    private static final SelectionBuilder SELECTION_BUILDER = Selection.builder().withMaxLimit(100).withDefaultLimit(10);
    private static final AtlasErrorSummary NOT_FOUND = new AtlasErrorSummary(new NullPointerException())
            .withMessage("No Task exists with the provided ID")
            .withErrorCode("Task not found")
            .withStatusCode(HttpStatusCode.NOT_FOUND);
    private static final AtlasErrorSummary FORBIDDEN = new AtlasErrorSummary(new NullPointerException())
            .withMessage("You require an API key to view this data")
            .withErrorCode("Api Key required")
            .withStatusCode(HttpStatusCode.FORBIDDEN);
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final TaskStore taskStore;
    private final NumberToShortStringCodec idCodec;
    
    public TaskController(ApplicationConfigurationFetcher configFetcher, AdapterLog log,
            AtlasModelWriter<Iterable<Task>> outputter, TaskStore taskStore, NumberToShortStringCodec idCodec) {
        super(configFetcher, log, outputter);
        this.taskStore = checkNotNull(taskStore);
        this.idCodec = checkNotNull(idCodec);
    }

    @RequestMapping(value="/3.0/feeds/youview/{publisher}/tasks.json", method = RequestMethod.GET)
    public void transactions(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = false) String contentUri,
            @RequestParam(value = "remote_id", required = false) String remoteId,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "action", required = false) String action,
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "element_id", required = false) String elementId
            ) throws IOException {
        
        try {
            Selection selection = SELECTION_BUILDER.build(request);
            ApplicationConfiguration appConfig = appConfig(request);
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            TaskQuery taskQuery = queryFrom(publisher, selection, contentUri, remoteId, status, action, type, elementId);
            Iterable<Task> allTasks = taskStore.allTasks(taskQuery);
            
            modelAndViewFor(request, response, allTasks, appConfig);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
    
    private TaskQuery queryFrom(Publisher publisher, Selection selection, String contentUri, String remoteId, String statusStr, 
            String actionStr, String typeStr, String elementId) {
        
        if (contentUri != null 
                && !contentUri.startsWith(NITRO_URI_PREFIX)) {
            contentUri = NITRO_URI_PREFIX + contentUri;
        }
        
        TaskQuery.Builder query = TaskQuery.builder(selection, publisher)
                .withContentUri(contentUri)
                .withRemoteId(remoteId)
                .withElementId(elementId);
        
        if (statusStr != null) {
            Status status = Status.valueOf(statusStr.trim().toUpperCase());
            query.withTaskStatus(status);
        }
        if (actionStr != null) {
            Action action = Action.valueOf(actionStr.trim().toUpperCase());
            query.withTaskAction(action);
        }
        if (typeStr != null) {
            TVAElementType type = TVAElementType.valueOf(typeStr.trim().toUpperCase());
            query.withTaskType(type);
        }
        
        return query.build();
    }

    @RequestMapping(value="/3.0/feeds/youview/{publisher}/tasks/{id}.json", method = RequestMethod.GET)
    public void task(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("publisher") String publisherStr,
            @PathVariable("id") String id) throws IOException {
        try {
            
            String rawPublisherStr = publisherStr.trim().toUpperCase();
            log.debug("tasks accessed with publisher {}", rawPublisherStr);
            Publisher publisher = Publisher.valueOf(rawPublisherStr);
            ApplicationConfiguration appConfig = appConfig(request);
            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            Optional<Task> resolved = taskStore.taskFor(idCodec.decode(id).longValue());
            if (!resolved.isPresent()) {
                errorViewFor(request, response, NOT_FOUND);
                return;
            }
            modelAndViewFor(request, response, ImmutableList.of(resolved.get()), appConfig);
        } catch (Exception e) {
            errorViewFor(request, response, AtlasErrorSummary.forException(e));
        }
    }
}
