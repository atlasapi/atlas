package org.atlasapi.remotesite.btvod.model;


public class BtVodImage {

    private String plproduct$mediaFileId;
    private Integer plproduct$height;
    private Integer plproduct$width;
    private String plproduct$url;

    public BtVodImage() {
    }

    public String getPlproduct$mediaFileId() {
        return plproduct$mediaFileId;
    }

    public void setPlproduct$mediaFileId(String plproduct$mediaFileId) {
        this.plproduct$mediaFileId = plproduct$mediaFileId;
    }

    public Integer getPlproduct$height() {
        return plproduct$height;
    }

    public void setPlproduct$height(Integer plproduct$height) {
        this.plproduct$height = plproduct$height;
    }

    public Integer getPlproduct$width() {
        return plproduct$width;
    }

    public void setPlproduct$width(Integer plproduct$width) {
        this.plproduct$width = plproduct$width;
    }

    public String getPlproduct$url() {
        return plproduct$url;
    }

    public void setPlproduct$url(String plproduct$url) {
        this.plproduct$url = plproduct$url;
    }

    @Override
    public String toString() {
        return "BtVodImage{" +
                "plproduct$mediaFileId='" + plproduct$mediaFileId + '\'' +
                ", plproduct$height='" + plproduct$height + '\'' +
                ", plproduct$width='" + plproduct$width + '\'' +
                ", plproduct$url='" + plproduct$url + '\'' +
                '}';
    }
}
