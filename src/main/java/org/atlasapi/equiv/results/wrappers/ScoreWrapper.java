package org.atlasapi.equiv.results.wrappers;

public class ScoreWrapper {

    private String source;
    private double score;

    private ScoreWrapper(String source, double score) {
        this.source = source;
        this.score = score;
    }

    public static ScoreWrapper create(String source, double score) {
        return new ScoreWrapper(source, score);
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
