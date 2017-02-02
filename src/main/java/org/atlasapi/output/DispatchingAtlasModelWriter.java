package org.atlasapi.output;

import static com.metabroadcast.common.media.MimeType.TEXT_PLAIN;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.v3.ApplicationConfiguration;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.media.MimeType;

public class DispatchingAtlasModelWriter<T> implements AtlasModelWriter<T> {
    
    public static final <T> Builder<T> dispatchingModelWriter() {
        return new Builder<T>();
    }
    
    public static final class Builder<T> {
        
        private ImmutableList.Builder<MappedWriter<T>> writers = ImmutableList.builder();
        
        public Builder<T> register(AtlasModelWriter<? super T> writer, String ext, MimeType contentType) {
            writers.add(new MappedWriter<T>(ext, contentType, writer));
            return this;
        }
        
        public AtlasModelWriter<T> build() {
            return new DispatchingAtlasModelWriter<T>(writers.build());
        }
        
    }

	private final Map<String, MappedWriter<T>> extensionMap;
	
	public DispatchingAtlasModelWriter(List<MappedWriter<T>> writers) {
        extensionMap = Maps.uniqueIndex(writers, new Function<MappedWriter<T>, String>() {
            @Override
            public String apply(MappedWriter<T> input) {
                return input.extension;
            }
        });
	}

	@Override
	public void writeError(HttpServletRequest request, HttpServletResponse response, AtlasErrorSummary exception) throws IOException {
		MappedWriter<T> writer = findWriterFor(request);
		if (writer != null) {
			writer.writeError(request, response, exception);
		} else {
		    writeNotFound(response);
		}
	}

    @Override
    public void writeTo(HttpServletRequest request, HttpServletResponse response, T graph, Set<Annotation> annotations, ApplicationConfiguration config) throws IOException {
        MappedWriter<T> writer = findWriterFor(request);
        if (writer != null) {
            writer.writeTo(request, response, graph, annotations, config);
        } else {
            writeNotFound(response);
        }
    }

	private static void addCorsHeader(HttpServletResponse response) {
        response.setHeader("Access-Control-Allow-Origin", "*");
    }

    private MappedWriter<T> findWriterFor(HttpServletRequest request) throws IOException {
		return extensionMap.get(extensionFrom(request.getRequestURI()));
	}

    private String extensionFrom(String requestUri) {
        String resource = requestUri.substring(requestUri.lastIndexOf("/"));
        int suffixStart = resource.indexOf(".");
        if (suffixStart >= 0) {
            return resource.substring(suffixStart);
        }
        return "";
    }

    public void writeNotFound(HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatusCode.NOT_FOUND.code());
		response.setCharacterEncoding(Charsets.UTF_8.toString());
		response.setContentType(TEXT_PLAIN.toString());
		addCorsHeader(response);
		response.getOutputStream().print("Not found");
    }
	
	private static class MappedWriter<T> implements AtlasModelWriter<T> {
		
		private final String extension;
		private final AtlasModelWriter<? super T> writer;
		private final MimeType mimeType;

		MappedWriter(String extension, MimeType mimeType, AtlasModelWriter<? super T> writer) {
			this.mimeType = mimeType;
			this.extension = "." + extension;
			this.writer = writer;
		}
		
        @Override
        public void writeTo(HttpServletRequest request, HttpServletResponse response, T graph, Set<Annotation> annotations, final ApplicationConfiguration config) throws IOException {
            response.setStatus(HttpStatusCode.OK.code());
            response.setCharacterEncoding(Charsets.UTF_8.toString());
            response.setContentType(mimeType.toString());
            addCorsHeader(response);
            writer.writeTo(request, response, graph, annotations, config);
        }

		@Override
		public void writeError(HttpServletRequest request, HttpServletResponse response, AtlasErrorSummary error) throws IOException {
			response.setStatus(error.statusCode().code());
			response.setCharacterEncoding(Charsets.UTF_8.toString());
			response.setContentType(mimeType.toString());
			addCorsHeader(response);
			writer.writeError(request, response, error);
		}

	}
}
