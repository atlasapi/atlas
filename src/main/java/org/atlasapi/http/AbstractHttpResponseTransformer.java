package org.atlasapi.http;

import com.google.common.base.Charsets;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;

public abstract class AbstractHttpResponseTransformer<T> implements HttpResponseTransformer<T> {

    @Override
    public T transform(HttpResponsePrologue prologue, InputStream body) throws HttpException, Exception {
        if(acceptableResponse(prologue)) {
            return transform(new InputStreamReader(body, prologue.getCharsetOrDefault(Charsets.UTF_8)));
        }
        throw new HttpException(prologue.statusCode() + " Response", prologue);
    }

    protected abstract T transform(InputStreamReader bodyReader) throws Exception;
    
    protected boolean acceptableResponse(HttpResponsePrologue prologue) {
        return acceptableResponseCodes().contains(prologue.statusCode());
    }

    protected Set<Integer> acceptableResponseCodes() {
        return ContiguousSet.create(Range.closedOpen(200, 400), DiscreteDomain.integers());
    }
    
}
