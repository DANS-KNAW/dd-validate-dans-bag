/*
 * Copyright (C) 2022 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.validatedansbag.core.rules;

import nl.knaw.dans.lib.util.XmlSchemaValidator;
import nl.knaw.dans.lib.util.ruleengine.NumberedRule;
import nl.knaw.dans.lib.util.ruleengine.RuleEngineImpl;
import nl.knaw.dans.lib.util.ruleengine.RuleValidationResult;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReaderImpl;
import nl.knaw.dans.validatedansbag.core.service.FileServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.FilesXmlServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsServiceImpl;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidatorImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import javax.xml.transform.Source;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static nl.knaw.dans.lib.util.ruleengine.RuleValidationResult.RuleValidationResultStatus.FAILURE;
import static nl.knaw.dans.lib.util.ruleengine.RuleValidationResult.RuleValidationResultStatus.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;

class LegacyVaasRuleSetTest {
    private static final String namespaces = """
        xmlns:dcterms="http://purl.org/dc/terms/"
        xmlns:files="http://easy.dans.knaw.nl/schemas/bag/metadata/files/"
        """;
    private static final String created = "Created: 2020-01-01T00:00:00.000Z\n";
    private static final String file = "<file filepath=\"data/payload.txt\"><dcterms:format>text/plain</dcterms:format></file>";

    @TempDir
    Path bag;

    private final XmlSchemaValidator schemas = Mockito.mock(XmlSchemaValidator.class);
    private List<NumberedRule> rules;

    @BeforeEach
    void setup() throws Exception {
        Files.createDirectories(bag.resolve("data"));
        Files.createDirectories(bag.resolve("metadata"));
        Files.writeString(bag.resolve("bagit.txt"), "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n");
        Files.writeString(bag.resolve("bag-info.txt"), created);
        var payload = "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(bag.resolve("data/payload.txt"), payload);
        Files.writeString(bag.resolve("manifest-md5.txt"), HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(payload)) + "  data/payload.txt\n");
        writeDataset("<dcterms:rightsHolder>Depositor</dcterms:rightsHolder>");
        writeFiles(file);

        var fileService = new FileServiceImpl(bag.getParent());
        var xmlReader = new XmlReaderImpl();
        rules = new RuleSets(null, fileService, new FilesXmlServiceImpl(xmlReader), new OriginalFilepathsServiceImpl(fileService), xmlReader,
            new BagItMetadataReaderImpl(), schemas, null, new IdentifierValidatorImpl(), new PolygonListValidatorImpl(), null, null, Map.of(), Map.of())
            .getLegacyVaasSet();
    }

    @Test
    void acceptsLegacyBagWithoutAipFieldsLicenseOrStrongManifest() throws Exception {
        var results = new RuleEngineImpl().validateBag(bag, rules);
        assertThat(results).noneMatch(r -> r.getStatus() == FAILURE);
        assertThat(results).filteredOn(r -> List.of("3.2.4(b)", "3.2.5").contains(r.getNumber()))
            .hasSize(2).allMatch(r -> r.getStatus() == SUCCESS);
        Mockito.verify(schemas).validateDocument(Mockito.any(Source.class), Mockito.eq("dataset.xml"));
        Mockito.verifyNoMoreInteractions(schemas);
    }

    @Test
    void validatesLegacyBagInfoFields() throws Exception {
        for (var entry : Map.of(
            "BagIt-Profile-Version: 1\n", "1.2.2(b)",
            "BagIt-Profile-URI: wrong\n", "1.2.3(b)",
            "Is-Version-Of: not-a-uuid\n", "1.2.5(b)",
            "BagIt-Profile-Version: 0\nBagIt-Profile-Version: 0\n", "1.2.2(a)",
            "Created: 2020-01-01T00:00:00.000Z\n", "1.2.4(a)"
        ).entrySet()) {
            Files.writeString(bag.resolve("bag-info.txt"), created + entry.getKey());
            assertThat(failures()).as(entry.getKey()).contains(entry.getValue());
        }
        Files.writeString(bag.resolve("bag-info.txt"), "Created: 2020-01-01\n");
        assertThat(failures()).contains("1.2.4(b)");
        Files.writeString(bag.resolve("bag-info.txt"), "BagIt-Profile-Version: 0\n");
        assertThat(failures()).contains("1.2.4(a)");
        Files.writeString(bag.resolve("bag-info.txt"), created + "BagIt-Profile-Version: 0\nBagIt-Profile-URI: doi:10.17026/dans-z52-ybfe\n");
        assertThat(failures()).isEmpty();
    }

    @Test
    void permitsLegacyMetadataButRejectsUnlistedFiles() throws Exception {
        for (var name : List.of("amd.xml", "emd.xml", "license.txt", "provenance.xml", "depositor-info/agreements.xml",
            "depositor-info/depositor-agreement.txt", "depositor-info/message-from-depositor.txt", "original/dataset.xml", "original/files.xml")) {
            Files.createDirectories(bag.resolve("metadata").resolve(name).getParent());
            Files.writeString(bag.resolve("metadata").resolve(name), "Not validated: sections 3.3, 3.4 and 4 are excluded");
        }
        assertThat(failures()).isEmpty();
        Files.writeString(bag.resolve("metadata/original/unlisted.txt"), "");
        assertThat(failures()).contains("2.5");
    }

    @Test
    void acceptsAuthorRightsHolderAndMultipleDois() throws Exception {
        writeDataset("""
            <dcx-dai:creator><dcx-dai:author><dcx-dai:role>RightsHolder</dcx-dai:role></dcx-dai:author></dcx-dai:creator>
            <dcterms:identifier xsi:type="id-type:DOI">10.1234/one</dcterms:identifier>
            <dcterms:identifier xsi:type="id-type:DOI">10.1234/two</dcterms:identifier>
            """);
        assertThat(failures()).isEmpty();
        writeDataset("<dcterms:identifier xsi:type=\"id-type:DOI\">bad-doi</dcterms:identifier>");
        assertThat(failures()).contains("3.1.3(b)", "3.1.10");
    }

    @Test
    void legacyLicenseIsOptionalButMustBeApprovedWhenUriTyped() throws Exception {
        var rights = "<dcterms:rightsHolder>Depositor</dcterms:rightsHolder>";
        for (var license : List.of("", "<dcterms:license>Custom license</dcterms:license>",
            "<dcterms:license xsi:type=\"dcterms:URI\">https://creativecommons.org/licenses/by/4.0/</dcterms:license>")) {
            writeDataset(rights + license);
            assertThat(failures()).as(license).isEmpty();
        }
        writeDataset(rights + "<dcterms:license xsi:type=\"dcterms:URI\">https://example.org/unapproved</dcterms:license>");
        assertThat(failures()).contains("3.1.2");
        writeDataset(rights + "<dcterms:license>one</dcterms:license><dcterms:license>two</dcterms:license>");
        assertThat(failures()).contains("3.1.2");
    }

    @Test
    void validatesFilesXmlWithoutNamespace() throws Exception {
        var invalidFiles = Map.of(
            "<other/>", "3.2.3",
            "<file><dcterms:format>text/plain</dcterms:format></file>", "3.2.4(a)",
            file.replace("data/payload.txt", "bagit.txt"), "3.2.4(b)",
            file + file, "3.2.5",
            "<file filepath=\"data/payload.txt\"/>", "3.2.6",
            file.replace("</file>", "<unknown xmlns=\"urn:unknown\"/></file>"), "3.2.7",
            file.replace("</file>", "<dcterms:accessRights>INVALID</dcterms:accessRights></file>"), "3.2.8",
            file.replace("</file>", "<files:visibleToRights>INVALID</files:visibleToRights></file>"), "3.2.8",
            file.replace("</file>", "<dcterms:accessRights>NONE</dcterms:accessRights><files:visibleToRights>ANONYMOUS</files:visibleToRights></file>"), "3.2.8"
        );
        for (var entry : invalidFiles.entrySet()) {
            writeFiles(entry.getKey());
            assertThat(failures()).as(entry.getKey()).contains(entry.getValue());
        }
        Files.writeString(bag.resolve("metadata/files.xml"), "<wrong/>");
        assertThat(failures()).contains("3.2.2");
        Files.writeString(bag.resolve("metadata/files.xml"), "<files>");
        assertThat(failures()).contains("3.2.1");
    }

    @Test
    void validatesNamespacedFilesAgainstSchema() throws Exception {
        Files.writeString(bag.resolve("metadata/files.xml"),
            "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" " + namespaces + ">" + file + "</files>");
        assertThat(failures()).isEmpty();
        Mockito.verify(schemas).validateDocument(Mockito.any(Source.class), Mockito.eq("files.xml"));
        Mockito.when(schemas.validateDocument(Mockito.any(Source.class), Mockito.eq("files.xml")))
            .thenReturn(List.of(new org.xml.sax.SAXParseException("Invalid files metadata", null)));
        assertThat(failures()).contains("3.2.1");
    }

    @Test
    void validatesOriginalPathMappingsAndUtf8() throws Exception {
        var mapping = bag.resolve("original-filepaths.txt");
        Files.writeString(mapping, "data/payload.txt\tdata/original name.txt\r\n");
        writeFiles(file.replace("data/payload.txt", "data/original name.txt"));
        assertThat(failures()).isEmpty();
        Files.writeString(mapping, "data/payload.txt\tdata/original name.txt\ndata/payload.txt\tdata/original name.txt\n");
        assertThat(failures()).contains("2.7.2");
        Files.writeString(mapping, "malformed mapping without matching paths\n");
        assertThat(failures()).contains("2.7.2");
        Files.writeString(mapping, "data/payload.txt\n");
        assertThat(failures()).contains("2.7.2");
        Files.write(mapping, new byte[] { (byte) 0xFF });
        assertThat(failures()).contains("2.7.1");
    }

    @Test
    void rejectsForbiddenPayloadPathCharacters() throws Exception {
        var fileService = Mockito.mock(nl.knaw.dans.validatedansbag.core.service.FileService.class);
        var rule = new BagPayloadPathsHaveNoForbiddenCharacters(fileService);
        Mockito.when(fileService.getAllFiles(bag.resolve("data"))).thenReturn(List.of(bag.resolve("data/bad:name.txt")));
        assertThat(rule.validate(bag).getStatus()).isEqualTo(nl.knaw.dans.lib.util.ruleengine.RuleResult.Status.ERROR);
        Mockito.when(fileService.getAllFiles(bag.resolve("data"))).thenReturn(List.of(bag.resolve("data/nested/good.txt")));
        assertThat(rule.validate(bag).getStatus()).isEqualTo(nl.knaw.dans.lib.util.ruleengine.RuleResult.Status.SUCCESS);
    }

    @Test
    void checksDistinctPolygonPointsAndLegacyRdCoordinates() throws Exception {
        var rights = "<dcterms:rightsHolder>Depositor</dcterms:rightsHolder>";
        writeDataset(rights + """
            <dcx-gml:spatial><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 1 1.0 1.0 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></dcx-gml:spatial>
            """);
        assertThat(failures()).contains("3.1.5");
        writeDataset(rights + """
            <dcx-gml:spatial><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList> 0 0 1 1 2 0 0 0 </gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></dcx-gml:spatial>
            """);
        assertThat(failures()).isEmpty();
        for (var point : List.of("<gml:Point><gml:pos>1 2</gml:pos></gml:Point>",
            "<gml:Envelope><gml:lowerCorner>1 2</gml:lowerCorner><gml:upperCorner>2 3</gml:upperCorner></gml:Envelope>")) {
            writeDataset(rights + "<dcterms:spatial srsName=\"http://www.opengis.net/def/crs/EPSG/0/28992\">" + point + "</dcterms:spatial>");
            assertThat(failures()).contains("3.1.7");
        }
        writeDataset(rights + """
            <dcterms:spatial srsName="http://www.opengis.net/def/crs/EPSG/0/28992"><gml:Point><gml:pos>
              100000 450000
            </gml:pos></gml:Point></dcterms:spatial>
            """);
        assertThat(failures()).isEmpty();
    }

    @Test
    void supportsBothDatasetNamespacesAndLocallyDeclaredIdentifierTypePrefixes() throws Exception {
        var rights = "<dcterms:rightsHolder>Depositor</dcterms:rightsHolder>";
        assertThat(failures()).isEmpty();
        var dataset = bag.resolve("metadata/dataset.xml");
        Files.writeString(dataset, Files.readString(dataset).replace("http://easy.dans.knaw.nl/schemas/md/ddm/", "http://schemas.dans.knaw.nl/dataset/ddm-v2/"));
        assertThat(failures()).isEmpty();
        for (var identifier : List.of(
            "<dc:identifier xmlns:ids=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xsi:type=\"ids:ARCHIS-ZAAK-IDENTIFICATIE\">12345678901</dc:identifier>",
            "<dcterms:identifier xmlns:ids=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xsi:type=\"ids:ARCHIS-ZAAK-IDENTIFICATIE\">12345678901</dcterms:identifier>")) {
            writeDataset(rights + identifier);
            assertThat(failures()).contains("3.1.8");
        }
        writeDataset(rights + "<dcterms:identifier xmlns:ids=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xsi:type=\"ids:DOI\">bad-doi</dcterms:identifier>");
        assertThat(failures()).contains("3.1.3(b)");
    }

    private List<String> failures() throws Exception {
        return new RuleEngineImpl().validateBag(bag, rules).stream()
            .filter(r -> r.getStatus() == FAILURE).map(RuleValidationResult::getNumber).toList();
    }

    private void writeFiles(String contents) throws Exception {
        Files.writeString(bag.resolve("metadata/files.xml"), "<files " + namespaces + ">" + contents + "</files>");
    }

    private void writeDataset(String contents) throws Exception {
        Files.writeString(bag.resolve("metadata/dataset.xml"), """
            <ddm:DDM xmlns:ddm="http://easy.dans.knaw.nl/schemas/md/ddm/"
              xmlns:dc="http://purl.org/dc/elements/1.1/"
              xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
              xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
              xmlns:gml="http://www.opengis.net/gml"
              xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
              xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/">
              <ddm:dcmiMetadata>
            """ + contents + "</ddm:dcmiMetadata></ddm:DDM>");
    }
}
