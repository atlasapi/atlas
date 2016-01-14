package org.atlasapi.remotesite.btvod;

public enum BtVodProductType {
    SEASON("season"),
    COLLECTION("collection"),
    HELP("help"),
    EPISODE("episode"),
    FILM("film"),
    MUSIC("music"),
    ;

    private final String productType;

    BtVodProductType(String productType) {
        this.productType = productType;
    }

    public boolean isOfType(String productType) {
        return productType != null && this.productType.equals(productType.toLowerCase());
    }
}
