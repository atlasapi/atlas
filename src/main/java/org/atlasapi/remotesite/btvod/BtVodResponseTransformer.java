package org.atlasapi.remotesite.btvod;

import com.google.gson.Gson;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.HttpStatusCode;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;

import java.io.InputStream;
import java.io.InputStreamReader;

public class BtVodResponseTransformer implements HttpResponseTransformer<BtVodResponse> {

    private final Gson gson = new Gson();
    @Override
    public BtVodResponse transform(HttpResponsePrologue prologue, InputStream body) throws Exception {
        if (prologue.statusCode() != HttpStatusCode.OK.code()) {
            throw new HttpException(
                    String.format(
                            "Error retrieving data from BT VoD feed: %s ResponseCode: %s",
                            prologue.finalUrl(),
                            prologue.statusCode()
                    ),
                    prologue
            );
        }

        return gson.fromJson(new InputStreamReader(body), BtVodResponse.class);
    }
}
