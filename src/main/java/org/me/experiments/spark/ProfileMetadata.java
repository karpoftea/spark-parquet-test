package org.me.experiments.spark;

public class ProfileMetadata {

    private String version;
    private Long created;
    private String guId;

    public ProfileMetadata() {
    }

    public ProfileMetadata(String version, Long created) {
        this.version = version;
        this.created = created;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Long getCreated() {
        return created;
    }

    public void setCreated(Long created) {
        this.created = created;
    }

    public String getGuId() {
        return guId;
    }

    public void setGuId(String guId) {
        this.guId = guId;
    }
}
