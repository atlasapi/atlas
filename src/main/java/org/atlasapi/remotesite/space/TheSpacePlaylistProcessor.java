package org.atlasapi.remotesite.space;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.http.SimpleHttpClient;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 */
public class TheSpacePlaylistProcessor {

    private final String BASE_CANONICAL_URI = "http://thespace.org";
    private final String BASE_ITEMS_URI = "http://thespace.org/items/";
    private final String BASE_CATEGORY_URI = "http://thespace.org/by/genre/";
    //
    private final Function<Content, ChildRef> CONTENT_TO_CHILD_REF = new ContentToChildRef();
    //
    private final SimpleHttpClient client;
    private final AdapterLog log;
    private final ContentResolver contentResolver;
    private final ContentWriter contentWriter;
    private final ContentGroupResolver groupResolver;
    private final ContentGroupWriter groupWriter;

    public TheSpacePlaylistProcessor(SimpleHttpClient client, AdapterLog log, ContentResolver contentResolver, ContentWriter contentWriter, ContentGroupResolver groupResolver, ContentGroupWriter groupWriter) {
        this.client = client;
        this.log = log;
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.groupResolver = groupResolver;
        this.groupWriter = groupWriter;
    }

    public void process(JsonNode item) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //
        String pid = item.get("pid").asText();
        //
        ContentGroup playlist = (ContentGroup) groupResolver.findByCanonicalUris(ImmutableSet.of(getItemsUri(pid))).getFirstValue().valueOrNull();
        if (playlist == null) {
            playlist = new ContentGroup(getItemsUri(pid));
            playlist.setType(ContentGroup.Type.PLAYLIST);
        }
        makePlaylist(playlist, item, mapper);
    }

    private void makePlaylist(ContentGroup playlist, JsonNode node, ObjectMapper mapper) throws Exception {
        try {
            JsonNode pid = node.get("pid");
            playlist.setCanonicalUri(getItemsUri(pid.asText()));
            playlist.setPublisher(Publisher.THESPACE);

            JsonNode title = node.get("title");
            if (title != null && !title.isNull()) {
                playlist.setTitle(title.asText());
            }

            JsonNode long_synopsis = node.get("long_synopsis");
            JsonNode medium_synopsis = node.get("medium_synopsis");
            JsonNode short_synopsis = node.get("short_synopsis");
            String synopsis = null;
            if (long_synopsis != null && !long_synopsis.isNull()) {
                synopsis = long_synopsis.asText();
            } else if (medium_synopsis != null && !medium_synopsis.isNull()) {
                synopsis = medium_synopsis.asText();
            } else if (short_synopsis != null && !short_synopsis.isNull()) {
                synopsis = short_synopsis.asText();
            }
            playlist.setDescription(synopsis);

            JsonNode image = node.get("image");
            if (image != null && !image.isNull()) {
                JsonNode smallImage = image.get("depiction_320");
                if (smallImage != null && !smallImage.isNull()) {
                    playlist.setThumbnail(getImagesUri(smallImage.asText()));
                }
                JsonNode bigImage = image.get("depiction_640");
                if (bigImage != null && !bigImage.isNull()) {
                    playlist.setImage(getImagesUri(bigImage.asText()));
                }
            }

            JsonNode categories = node.get("categories");
            if (categories != null && !categories.isNull()) {
                Iterator<JsonNode> it = categories.getElements();
                Set<String> genres = new HashSet<String>();
                while (it.hasNext()) {
                    String id = BASE_CATEGORY_URI + it.next().get("id").asText();
                    genres.add(id);
                }
                playlist.setGenres(new TheSpaceGenreMap().mapRecognised(genres));
            }

            Iterable<Content> contents = getContents(node);
            playlist.setContents(Iterables.transform(contents, CONTENT_TO_CHILD_REF));

            groupWriter.createOrUpdate(playlist);

            for (Content content : contents) {
                if (!content.getContentGroupRefs().contains(playlist.contentGroupRef())) {
                    content.addContentGroup(playlist.contentGroupRef());
                    if (content instanceof Item) {
                        contentWriter.createOrUpdate((Item) content);
                    } else if (content instanceof Container) {
                        contentWriter.createOrUpdate((Container) content);
                    } else {
                        throw new IllegalStateException("Unexpected content type: " + content.getClass().getName());
                    }
                }
            }
        } catch (Exception ex) {
            log.record(new AdapterLogEntry(AdapterLogEntry.Severity.WARN).withDescription("Failed ingesting playlist: " + playlist.getCanonicalUri()).withSource(getClass()));
            throw ex;
        }
    }

    private Iterable<Content> getContents(JsonNode node) throws Exception {
        List<Content> result = new LinkedList<Content>();
        JsonNode contents = node.get("children");
        if (contents != null && !contents.isNull()) {
            Iterator<JsonNode> it = contents.getElements();
            while (it.hasNext()) {
                JsonNode content = it.next();
                String cPid = content.get("pid").asText();
                ResolvedContent episode = contentResolver.findByCanonicalUris(ImmutableList.of(getItemsUri(cPid)));
                if (!episode.isEmpty()) {
                    result.add((Content) episode.getFirstValue().requireValue());
                }
            }
        }
        return result;
    }

    private String getImagesUri(String image) {
        return BASE_CANONICAL_URI + image;
    }
    
    private String getItemsUri(String pid) {
        return BASE_ITEMS_URI + pid;
    }

    private class ContentToChildRef implements Function<Content, ChildRef> {

        @Override
        public ChildRef apply(Content input) {
            return input.childRef();
        }
    }
}
