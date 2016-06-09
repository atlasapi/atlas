package org.atlasapi.views;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.activation.FileTypeMap;

import org.springframework.http.MediaType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.view.ContentNegotiatingViewResolver;

/**
 * Specialisation of {@link ContentNegotiatingViewResolver} that can extract multi-dot extensions,
 * rather than just the last part.
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class ExtensionBasedContentNegotiatingViewResolver extends ContentNegotiatingViewResolver {

	private ConcurrentMap<String, MediaType> mediaTypes = new ConcurrentHashMap<String, MediaType>();

	protected MediaType getMediaTypeFromFilename(String filename) {
		String extension = getExtension(filename);
		if (!StringUtils.hasText(extension)) {
			return null;
		}
		extension = extension.toLowerCase(Locale.ENGLISH);
		MediaType mediaType = mediaTypes.get(extension);
		if (mediaType == null) {
			return super.getMediaTypeFromFilename(filename);
		}
		return mediaType;
	}

	private String getExtension(String path) {
		if (path == null) {
			return null;
		}
		int sepIndex = path.indexOf(".");
		return (sepIndex != -1 ? path.substring(sepIndex + 1) : null);
	}

	/**
	 * Sets the mapping from file extensions to media types.
	 * <p>When this mapping is not set or when an extension is not present, this view resolver
	 * will fall back to using a {@link FileTypeMap} when the Java Action Framework is available.
	 */
	public void setMediaTypes(Map<String, String> mediaTypes) {
		super.setMediaTypes(mediaTypes);
		Assert.notNull(mediaTypes, "'mediaTypes' must not be null");
		for (Map.Entry<String, String> entry : mediaTypes.entrySet()) {
			String extension = entry.getKey().toLowerCase(Locale.ENGLISH);
			MediaType mediaType = MediaType.parseMediaType(entry.getValue());
			this.mediaTypes.put(extension, mediaType);
		}
	}


}
