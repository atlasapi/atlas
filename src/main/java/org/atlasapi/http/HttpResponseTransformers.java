package org.atlasapi.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.metabroadcast.common.http.HttpResponseTransformer;
import org.atlasapi.remotesite.html.HtmlNavigator;

import java.io.InputStreamReader;
import java.lang.reflect.Type;

public class HttpResponseTransformers {

    private HttpResponseTransformers() {
    }
    
    public static <T> HttpResponseTransformer<T> gsonResponseTransformer(GsonBuilder gsonBuilder, Class<? extends T> cls) {
        return new GsonHttpResponseTransformer<T>(gsonBuilder, TypeToken.get(cls).getType());
    }
    
    public static <T> HttpResponseTransformer<T> gsonResponseTransformer(GsonBuilder gsonBuilder, TypeToken<? extends T> token) {
        return new GsonHttpResponseTransformer<T>(gsonBuilder, token.getType());
    }

    public static class GsonHttpResponseTransformer<T> extends AbstractHttpResponseTransformer<T> {


        public GsonHttpResponseTransformer(GsonBuilder gsonBuilder, Type type) {
            this.type = type;
            this.gson = gsonBuilder.create();
        }
        
        private final Type type;
        private final Gson gson;

        @Override
        public T transform(InputStreamReader in) throws Exception {
            return gson.fromJson(in, type);
        }
    }
    
    public static HttpResponseTransformer<HtmlNavigator> htmlNavigatorTransformer() {
        return htmlNavigatorInstance;
    }
    
    private static final HttpResponseTransformer<HtmlNavigator> htmlNavigatorInstance = new HtmlNavigatorHttpResponseTransformer();

    public static class HtmlNavigatorHttpResponseTransformer extends AbstractHttpResponseTransformer<HtmlNavigator> {

        @Override
        public HtmlNavigator transform(InputStreamReader in) throws Exception {
            return new HtmlNavigator(in);
        }
    }
}