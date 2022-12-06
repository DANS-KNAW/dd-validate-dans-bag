package nl.knaw.dans.validatedansbag.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class XmlSchemaConfig {

    @NotNull
    @JsonProperty("dataset.xml")
    private URI datasetXml;
    @NotNull
    @JsonProperty("files.xml")

    private URI filesXml;

    @NotNull
    @JsonProperty("agreements.xml")

    private URI agreementsXml;

    @NotNull
    @JsonProperty("provenance.xml")

    private URI provenanceXml;

    @NotNull
    @JsonProperty("amd.xml")

    private URI amdXml;

    @NotNull
    @JsonProperty("emd.xml")

    private URI emdXml;

    public Map<String, URI> buildMap() {
        var map = new HashMap<String, URI>();
        map.put("dataset.xml", datasetXml);
        map.put("files.xml", filesXml);
        map.put("agreements.xml", agreementsXml);
        map.put("provenance.xml", provenanceXml);
        map.put("amd.xml", amdXml);
        map.put("emd.xml", emdXml);
        return map;
    }

    public URI getDatasetXml() {
        return datasetXml;
    }

    public void setDatasetXml(URI datasetXml) {
        this.datasetXml = datasetXml;
    }

    public URI getFilesXml() {
        return filesXml;
    }

    public void setFilesXml(URI filesXml) {
        this.filesXml = filesXml;
    }

    public URI getAgreementsXml() {
        return agreementsXml;
    }

    public void setAgreementsXml(URI agreementsXml) {
        this.agreementsXml = agreementsXml;
    }

    public URI getProvenanceXml() {
        return provenanceXml;
    }

    public void setProvenanceXml(URI provenanceXml) {
        this.provenanceXml = provenanceXml;
    }

    public URI getAmdXml() {
        return amdXml;
    }

    public void setAmdXml(URI amdXml) {
        this.amdXml = amdXml;
    }

    public URI getEmdXml() {
        return emdXml;
    }

    public void setEmdXml(URI emdXml) {
        this.emdXml = emdXml;
    }
}
