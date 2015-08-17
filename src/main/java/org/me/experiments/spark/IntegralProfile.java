package org.me.experiments.spark;

import java.util.Map;

public class IntegralProfile {

    private ProfileMetadata metadata;

    private Map<String, Map<String, ?>> ids;

    private Map<String, Map<String, Attribute>> attributes;

    private Map<String, Map<String, Interest>> interests;

    public ProfileMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(ProfileMetadata metadata) {
        this.metadata = metadata;
    }

    public Map<String, Map<String, ?>> getIds() {
        return ids;
    }

    public void setIds(Map<String, Map<String, ?>> ids) {
        this.ids = ids;
    }

    public Map<String, Map<String, Attribute>> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Map<String, Attribute>> attributes) {
        this.attributes = attributes;
    }

    public Map<String, Map<String, Interest>> getInterests() {
        return interests;
    }

    public void setInterests(Map<String, Map<String, Interest>> interests) {
        this.interests = interests;
    }
}
