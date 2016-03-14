package org.atlasapi.query.v2;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.query.ApplicationConfigurationFetcher;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.feeds.tasks.Action;
import org.atlasapi.feeds.tasks.Destination.DestinationType;
import org.atlasapi.feeds.tasks.Status;
import org.atlasapi.feeds.tasks.TVAElementType;
import org.atlasapi.feeds.tasks.Task;
import org.atlasapi.feeds.tasks.TaskQuery;
import org.atlasapi.feeds.tasks.persistence.TaskStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.AtlasErrorSummary;
import org.atlasapi.output.AtlasModelWriter;
import org.atlasapi.persistence.logging.AdapterLog;

import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;

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
    private static final AtlasErrorSummary INVALID_DESTINATION_TYPE = new AtlasErrorSummary(new NullPointerException())
            .withMessage("No Feed exists of the provided type")
            .withErrorCode("Feed Type not found")
            .withStatusCode(HttpStatusCode.NOT_FOUND);
    
    private final TaskStore taskStore;
    private final NumberToShortStringCodec idCodec;
    
    public TaskController(ApplicationConfigurationFetcher configFetcher, AdapterLog log,
            AtlasModelWriter<Iterable<Task>> outputter, TaskStore taskStore, NumberToShortStringCodec idCodec) {
        super(configFetcher, log, outputter);
        this.taskStore = checkNotNull(taskStore);
        this.idCodec = checkNotNull(idCodec);
    }

    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}/tasks.json", method = RequestMethod.GET)
    public void transactions(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @RequestParam(value = "uri", required = false) String contentUri,
            @RequestParam(value = "remote_id", required = false) String remoteId,
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "action", required = false) String action,
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "element_id", required = false) String elementId,
            @RequestParam(value = "order_by", required = false) String orderBy
    ) throws IOException {
        try {
            Selection selection = SELECTION_BUILDER.build(request);
            ApplicationConfiguration appConfig = appConfig(request);
            Publisher publisher = Publisher.valueOf(publisherStr.trim().toUpperCase());
            DestinationType destinationType = parseDestinationFrom(destinationTypeStr);

            if (destinationType == null) {
                errorViewFor(request, response, INVALID_DESTINATION_TYPE);
                return;
            }

            if (!appConfig.isEnabled(publisher)) {
                errorViewFor(request, response, FORBIDDEN);
                return;
            }

            TaskQuery taskQuery = queryFrom(destinationType, publisher, selection, contentUri,
                    remoteId, status, action, type, elementId, orderBy);

            Iterable<Task> allTasks = taskStore.allTasks(taskQuery);

            modelAndViewFor(request, response, allTasks, appConfig);
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

    private TaskQuery queryFrom(DestinationType destinationType, Publisher publisher, Selection selection, 
            String contentUri, String remoteId, String statusStr, String actionStr, String typeStr, 
            String elementId, String orderBy) {

        if (contentUri != null 
                && !contentUri.startsWith(NITRO_URI_PREFIX)) {
            contentUri = NITRO_URI_PREFIX + contentUri;
        }

        TaskQuery.Builder query = TaskQuery.builder(selection, publisher, destinationType)
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

        if (orderBy != null) {
            query.withSort(parseOrderBy(orderBy));
        } else {
            query.withSort(TaskQuery.Sort.of(
                    TaskQuery.Sort.Field.UPLOAD_TIME,
                    TaskQuery.Sort.Direction.DESC
            ));
        }

        return query.build();
    }

    private TaskQuery.Sort parseOrderBy(String orderBy) {
        String[] parts = orderBy.split("\\.");
        if (parts.length == 1) {
            return TaskQuery.Sort.of(
                    TaskQuery.Sort.Field.fromKey(parts[0])
            );
        } else if (parts.length == 2) {
            return TaskQuery.Sort.of(
                    TaskQuery.Sort.Field.fromKey(parts[0]),
                    TaskQuery.Sort.Direction.valueOf(parts[1].toUpperCase())
            );
        } else {
            throw new IllegalArgumentException(String.format(
                    "Illegal orderBy value: %s",
                    orderBy
            ));
        }
    }

    @RequestMapping(value="/3.0/feeds/{destinationType}/{publisher}/tasks/{id}.json", method = RequestMethod.GET)
    public void task(HttpServletRequest request, HttpServletResponse response,
            @PathVariable("destinationType") String destinationTypeStr,
            @PathVariable("publisher") String publisherStr,
            @PathVariable("id") String id) throws IOException {
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
