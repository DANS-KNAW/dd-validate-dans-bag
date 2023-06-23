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

import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReader;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsService;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import nl.knaw.dans.validatedansbag.core.service.XmlSchemaValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class XmlRulesImplTest {

    final FileService fileService = Mockito.mock(FileService.class);
    final BagItMetadataReader bagItMetadataReader = Mockito.mock(BagItMetadataReader.class);
    final XmlSchemaValidator xmlSchemaValidator = Mockito.mock(XmlSchemaValidator.class);
    final OriginalFilepathsService originalFilepathsService = Mockito.mock(OriginalFilepathsService.class);

    @AfterEach
    void afterEach() {
        Mockito.reset(fileService);
        Mockito.reset(bagItMetadataReader);
        Mockito.reset(xmlSchemaValidator);
        Mockito.reset(originalFilepathsService);
    }

    private Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(str);
    }

    @Test
    void xmlFileConformsToSchema_should_return_SUCCESS_status_if_file_validates_with_xsd() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>PAN-00008136 - knobbed sickle</dc:title>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());
        Mockito.doReturn(new ArrayList<SAXParseException>())
            .when(xmlSchemaValidator).validateDocument(Mockito.any(), Mockito.anyString());

        var result = new XmlFileConformsToSchema(Path.of("metadata/dataset.xml"), reader,"ddm", xmlSchemaValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void xmlFileConformsToSchema_should_return_ERROR_status_when_file_does_not_validate_with_xsd() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <ddm:profile>\n"
            + "        <dcterms:description xml:lang=\"en\">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        Mockito.doReturn(List.of(new SAXParseException("msg", null)))
            .when(xmlSchemaValidator).validateDocument(Mockito.any(), Mockito.anyString());

        var result = new XmlFileConformsToSchema(Path.of("metadata/dataset.xml"), reader,"ddm", xmlSchemaValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void xmlFileConformsToSchema_should_return_ERROR_status_when_xmlReader_throws_exception() throws Exception {
        var reader = Mockito.mock(XmlReader.class);

        Mockito.doThrow(new SAXParseException("Invalid XML", null))
            .when(reader).readXmlFile(Mockito.any());

        var result = new XmlFileConformsToSchema(Path.of("metadata/dataset.xml"), reader,"ddm", xmlSchemaValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void xmlFileIfExistsConformsToSchema_should_return_ERROR_status_if_file_exists_but_does_not_validate() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "    <ddm:profile>\n"
            + "        <dcterms:description xml:lang=\"en\">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(true).when(fileService).exists(Path.of("bagdir/metadata/dataset.xml"));
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());
        Mockito.doReturn(List.of(new SAXParseException("msg", null)))
            .when(xmlSchemaValidator).validateDocument(Mockito.any(), Mockito.anyString());

        var result = new XmlFileIfExistsConformsToSchema(Path.of("metadata/dataset.xml"), reader, "ddm", xmlSchemaValidator, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void xmlFileIfExistsConformsToSchema_should_return_SKIP_DEPENDENCIES_status_if_file_does_not_exist() throws Exception {
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(false).when(fileService).exists(Path.of("bagdir/metadata/dataset.xml"));
        var result = new XmlFileIfExistsConformsToSchema(Path.of("metadata/dataset.xml"), reader, "ddm", xmlSchemaValidator, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SKIP_DEPENDENCIES, result.getStatus());

    }
}
