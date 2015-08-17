package org.me.experiments.spark;

public class Interest {

    private Long taxId;
    private Float weight;

    public Interest(Long taxId, Float weight) {
        this.taxId = taxId;
        this.weight = weight;
    }

    public Long getTaxId() {
        return taxId;
    }

    public void setTaxId(Long taxId) {
        this.taxId = taxId;
    }

    public Float getWeight() {
        return weight;
    }

    public void setWeight(Float weight) {
        this.weight = weight;
    }
}
