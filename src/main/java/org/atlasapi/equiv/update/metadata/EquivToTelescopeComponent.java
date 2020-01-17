package org.atlasapi.equiv.update.metadata;

import com.google.common.base.Strings;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.HashMap;

public class EquivToTelescopeComponent {

    private static final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private String componentName;
    private HashMap<String, String> componentResults;

    private EquivToTelescopeComponent() {
        componentResults = new HashMap<>();
    }

    public static EquivToTelescopeComponent create() {
        return new EquivToTelescopeComponent();
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public void addComponentResult(@Nullable Long longId, String score) {
        if (longId == null) {
            return;
        }
        String id = codec.encode(BigInteger.valueOf(longId));
        componentResults.put(id, score);
    }

    public void addComponentResult(@Nullable String id, String score) {
        if (Strings.isNullOrEmpty(id)) {
            return;
        }
        componentResults.put(id, score);
    }

    public String getComponentName() {
        return componentName;
    }

    public HashMap<String, String> getComponentResults() {
        return componentResults;
    }
}
