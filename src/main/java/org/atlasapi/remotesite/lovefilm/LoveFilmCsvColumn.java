package org.atlasapi.remotesite.lovefilm;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.remotesite.lovefilm.LoveFilmData.LoveFilmDataRow;

public enum LoveFilmCsvColumn {
    
    ACCESS_METHOD("access_method"),
    BBFC_RATING("bbfc_rating"),
    CONTRIBUTOR("contributor"),
    CURRENCY("currency"),
    DEVELOPER("developer"),
    DIGITAL_RELEASES_IDS("digital_releases_ids"),
    DM_RATING("dm_rating"),
    DRM_RIGHTS("drm_rights"),
    ENTITY("entity"),
    EPISODE_SEQUENCE("episode_sequence"),
    EXTERNAL_ID("external_id"),
    EXTERNAL_PRODUCT_DESCRIPTION_URL("external_product_description_url"),
    FORMAT("format"),
    FSK_RATING("fsk_rating"),
    GENRE("genre"),
    HD_AVAILABLE("hd_available"),
    HEROSHOT_URL("heroshot_url"),
    IMAGE_URL("image_url"),
    IS_ADULT_PRODUCT("is_adult_product"),
    ITEM_NAME("item_name"),
    ITEM_TYPE_KEYWORD("item_type_keyword"),
    LANGUAGE("language"),
    MANUFACTURER_ITEM_ID("Manufacturer_item_id"),
    MSA("msa"),
    NEXT_EPISODE("next_episode"),
    NM_RATING("nm_rating"),
    NUMBER_OF_DISKS("number_of_disks"),
    ORIGINAL_PUBLICATION_DATE("Original_publication_date"),
    PEGI_RATING("pegi_rating"),
    PLATFORM("platform"),
    PRICE("price"),
    PRODUCT_SITE_LAUNCH_DATE("Product_site_launch_date"),
    RELEASE_WINDOW_END_DATE("release_window_end_date"),
    RUN_TIME_SEC("run_time_sec"),
    SERIES_ID("series_id"),
    SHOW_ID("show_id"),
    SKU("sku"),
    SM_RATING("sm_rating"),
    STUDIO("studio"),
    TITLE_MASTER_ID("title_master_id");

    private String name;

    private LoveFilmCsvColumn(String name) {
        this.name = name;
    }
    
    public String valueFrom(LoveFilmDataRow row) {
        return checkNotNull(row.getColumnValue(name));
    }
    
    public boolean valueIs(LoveFilmDataRow row, String expected) {
        return row.columnValueIs(name, expected);
    }
}
