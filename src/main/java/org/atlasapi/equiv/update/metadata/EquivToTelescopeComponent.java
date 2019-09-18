package org.atlasapi.equiv.update.metadata;

import java.math.BigInteger;
import java.util.HashMap;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.base.Strings;

public class EquivToTelescopeComponent {

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

    public void addComponentResult(long longId, String score) {
        String id = new SubstitutionTableNumberCodec().lowerCaseOnly().encode(
                BigInteger.valueOf(longId)
        );
        componentResults.put(id, score);
    }

    public void addComponentResult(String id, String score) {
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
