package org.atlasapi.remotesite.util;

import java.util.Locale;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

public class EnglishLanguageCodeMap {

    private static EnglishLanguageCodeMap INSTANCE;

    public static EnglishLanguageCodeMap getInstance() {
        if(INSTANCE == null) { //not thread safe, but we don't care.
            INSTANCE = new EnglishLanguageCodeMap();
        }

        return INSTANCE;
    }

    private Map<String, Optional<String>> languageMap;
    private EnglishLanguageCodeMap() {
        this.languageMap = initializeMap();
    }

    private Map<String, Optional<String>> initializeMap() {
        Map<String, Optional<String>> languageCode = Maps.newHashMap();
        for (String code : Locale.getISOLanguages()) {
            languageCode.put(new Locale(code).getDisplayLanguage(Locale.ENGLISH).toLowerCase(),Optional.of(code));
        }

        addCustomLanguages(languageCode);

        return languageCode;
    }
    
    public Optional<String> codeForEnglishLanguageName(String englishName) {
        Optional<String> possibleCode = languageMap.get(englishName);
        return possibleCode == null ? Optional.<String>absent() : possibleCode; 
    }

    private void addCustomLanguages(Map<String, Optional<String>> languageCode) {
        languageCode.put("cantonese", Optional.of("zh"));
        languageCode.put("mandarin", Optional.of("zh"));
        languageCode.put("singdarin", Optional.of("zh"));
        languageCode.put("hokkien", Optional.of("zh"));
        languageCode.put("odia", Optional.of("or"));
    }
}
