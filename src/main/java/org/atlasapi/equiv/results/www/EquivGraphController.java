package org.atlasapi.equiv.results.www;

import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.model.SimpleModel;
import com.metabroadcast.common.model.SimpleModelList;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class EquivGraphController {

    private final LookupEntryStore lookupStore;
    private final SubstitutionTableNumberCodec codec;

    private EquivGraphController(LookupEntryStore lookupStore) {
        this.lookupStore = checkNotNull(lookupStore);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    public static EquivGraphController create(LookupEntryStore lookupStore) {
        return new EquivGraphController(lookupStore);
    }

    @RequestMapping("/system/equivalence/graph")
    public String graphEquiv(
            Map<String, Object> model,
            @RequestParam(value = "id", defaultValue = "") String id,
            @RequestParam(value = "uri", defaultValue = "") String uri,
            @RequestParam(value = "min_edges", required = false, defaultValue = "1") String minEdges
    ) {
        model.put("id", id);
        model.put("uri", uri);
        model.put("min_edges", Integer.parseInt(minEdges));

        return "equivalence.graph";
    }

    @RequestMapping("/system/equivalence/graph/data.json")
    public String graphData(
            Map<String, Object> model,
            @RequestParam(value = "id", defaultValue = "") String id,
            @RequestParam(value = "uri", defaultValue = "") String uri,
            @RequestParam(value = "min_edges", required = false, defaultValue = "1") String minEdges
    ) {
        if (Strings.isNullOrEmpty(uri) && Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("Must specify at least one of 'uri' or 'id'");
        }

        int minimumEdgeCount = Integer.parseInt(minEdges);

        model.put("id", id);
        model.put("uri", uri);
        model.put("min_edges", minimumEdgeCount);

        String uriFromId = "";
        if (getUriFromId(id).isPresent()) {
            uriFromId = getUriFromId(id).get();
        }

        LookupEntry subj = Iterables.getOnlyElement(
                lookupStore.entriesForCanonicalUris(ImmutableList.of(uri, uriFromId)),
                null
        );

        if (subj != null) {
            Iterable<LookupEntry> equivs = lookupStore.entriesForCanonicalUris(
                    subj.equivalents()
                            .stream()
                            .map(LookupRef.TO_URI::apply)
                            .collect(Collectors.toList()));

            List<SimpleModel> nodes = Lists.newLinkedList();

            for (LookupEntry equiv : equivs) {
                if (edgeCount(equiv) >= minimumEdgeCount) {
                    SimpleModel nodeModel = modelNode(equiv);
                    if (equiv.uri().equals(uri)) {
                        nodeModel.put("fixed", true);
                    }
                    nodes.add(nodeModel);
                }
            }

            model.put("content", new SimpleModelList(nodes));
        }

        return "equivalence.graph";
    }

    private Optional<String> getUriFromId(String id) {
        if (!Strings.isNullOrEmpty(id)) {
            Long decodedId = codec.decode(id).longValue();

            LookupEntry lookupEntry = Iterables.getOnlyElement(
                    lookupStore.entriesForIds(ImmutableList.of(decodedId))
            );

            return Optional.ofNullable(lookupEntry.uri());
        }

        return Optional.empty();
    }

    private int edgeCount(LookupEntry equiv) {
        return nonReflexiveIds(equiv, equiv.getNeighbours()).size();
    }

    private SimpleModel modelNode(LookupEntry equiv) {
        return new SimpleModel()
                .put("id", String.valueOf(equiv.id()))
                .put("uri", equiv.uri())
                .put("source", equiv.lookupRef().publisher().key())
                .putStrings("direct", nonReflexiveIds(equiv, equiv.directEquivalents().getLookupRefs()))
                .putStrings("explicit", nonReflexiveIds(equiv, equiv.explicitEquivalents().getLookupRefs()))
                .putStrings("blacklisted", nonReflexiveIds(equiv, equiv.blacklistedEquivalents().getLookupRefs()));
    }

    private Collection<String> nonReflexiveIds(
            LookupEntry equiv,
            Set<LookupRef> directEquivalents
    ) {
        return Collections2.filter(
                Collections2.transform(directEquivalents, LookupRef.TO_URI),
                Predicates.not(Predicates.equalTo(equiv.uri()))
        );
    }

}
