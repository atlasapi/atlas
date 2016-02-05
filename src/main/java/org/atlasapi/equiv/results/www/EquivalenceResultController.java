package org.atlasapi.equiv.results.www;

import static com.metabroadcast.common.http.HttpStatusCode.NOT_FOUND;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.results.probe.EquivalenceProbeStore;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.api.client.util.Strings;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.model.SimpleModel;
import com.metabroadcast.common.model.SimpleModelList;

@Controller
public class EquivalenceResultController {

    private final EquivalenceResultStore store;
    private final EquivalenceProbeStore probeStore;

    private final RestoredEquivalenceResultModelBuilder resultModelBuilder;
    private final ContentResolver contentResolver;
    private final LookupEntryStore lookupEntryStore;
    private final SubstitutionTableNumberCodec codec;

    public EquivalenceResultController(EquivalenceResultStore store, EquivalenceProbeStore probeStore,
            ContentResolver contentResolver, LookupEntryStore lookupEntryStore) {
        this.store = store;
        this.probeStore = probeStore;
        this.contentResolver = contentResolver;
        this.resultModelBuilder = new RestoredEquivalenceResultModelBuilder();
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    @RequestMapping(value = "/system/equivalence/result", method = RequestMethod.GET)
    public String showResult(Map<String, Object> model, HttpServletResponse response, 
            @RequestParam(value = "uri", required=false) String uri,
            @RequestParam(value = "id", required=false) String id) throws IOException {

        if (!(Strings.isNullOrEmpty(uri) ^ Strings.isNullOrEmpty(id))) {
            throw new IllegalArgumentException("Must specify exactly one of 'uri' parameter or 'id' parameter");
        }

        if (!Strings.isNullOrEmpty(id)) {
            uri = uriFor(id);
        }
        
        StoredEquivalenceResult equivalenceResult = store.forId(uri);

        if (equivalenceResult == null) {
            response.sendError(NOT_FOUND.code(), "No result for URI");
            return null;
        }

        SimpleModel resultModel = resultModelBuilder.build(equivalenceResult, probeStore.probeFor(uri));

        model.put("result", resultModel);

        return "equivalence.result";
    }

    private String uriFor(String id) {
        return Iterables.getOnlyElement(lookupEntryStore.entriesForIds(ImmutableSet.of(codec.decode(
                id).longValue()))).uri();
    }

    @RequestMapping(value = "/system/equivalence/results", method = RequestMethod.GET)
    public String showSubResults(Map<String, Object> model, HttpServletResponse response, @RequestParam(value = "uri", required = true) String uri) throws IOException {

        Maybe<Identified> ided = contentResolver.findByCanonicalUris(ImmutableList.of(uri)).get(uri);

        if (ided.isNothing()) {
            response.sendError(NOT_FOUND.code(), "Unknown URI");
            return null;
        }

        SimpleModelList resultModelList = new SimpleModelList();

        if (ided.requireValue() instanceof Container) {

            List<StoredEquivalenceResult> results = store.forIds(Iterables.transform(((Container) ided.requireValue()).getChildRefs(), new Function<ChildRef, String>() {
                @Override
                public String apply(ChildRef input) {
                    return input.getUri();
                }
            }));

            for (StoredEquivalenceResult result : results) {
                resultModelList.add(resultModelBuilder.build(result, probeStore.probeFor(uri)));
            }

        }
        model.put("results", resultModelList);
        return "equivalence.results";
    }
}
