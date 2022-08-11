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
package nl.knaw.dans.validatedansbag.core.service;

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.exceptions.InvalidBagitFileFormatException;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BagInfoCheckerImplTest {

    final FileService fileService = Mockito.mock(FileService.class);
    final BagXmlReader bagXmlReader = Mockito.mock(BagXmlReader.class);
    final DaiDigestCalculator daiDigestCalculator = new DaiDigestCalculatorImpl();
    final BagItMetadataReader bagItMetadataReader = Mockito.mock(BagItMetadataReader.class);
    final PolygonListValidator polygonListValidator = new PolygonListValidatorImpl();
    final XmlValidator xmlValidator = Mockito.mock(XmlValidator.class);
    final OriginalFilepathsService originalFilepathsService = Mockito.mock(OriginalFilepathsService.class);

    @AfterEach
    void afterEach() {
        Mockito.reset(fileService);
        Mockito.reset(bagXmlReader);
        Mockito.reset(bagItMetadataReader);
        Mockito.reset(xmlValidator);
        Mockito.reset(originalFilepathsService);
    }

    @Test
    void testBagIsValid() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.bagIsValid().validate(Path.of("testpath")));

        Mockito.verify(bagItMetadataReader).verifyBag(Path.of("testpath"));

    }

    @Test
    void testBagIsNotValidWithExceptionThrown() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.doThrow(new InvalidBagitFileFormatException("Invalid file format"))
            .when(bagItMetadataReader).verifyBag(Mockito.any());

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagIsValid().validate(Path.of("testpath")));
    }

    @Test
    void containsDirWorks() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isDirectory(Mockito.any()))
            .thenReturn(true);

        assertDoesNotThrow(() -> checker.containsDir(Path.of("testpath")).validate(Path.of("bagdir")));

        Mockito.verify(fileService).isDirectory(Path.of("bagdir/testpath"));
    }

    @Test
    void containsDirThrowsException() {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isDirectory(Mockito.any()))
            .thenReturn(false);

        assertThrows(RuleViolationDetailsException.class, () -> checker.containsDir(Path.of("testpath")).validate(Path.of("bagdir")));
    }

    @Test
    void containsFileWorks() {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(true);

        assertDoesNotThrow(() -> checker.containsFile(Path.of("testpath")).validate(Path.of("bagdir")));

        Mockito.verify(fileService).isFile(Path.of("bagdir/testpath"));
    }

    @Test
    void containsFileThrowsException() {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(false);

        assertThrows(RuleViolationDetailsException.class, () -> checker.containsFile(Path.of("testpath")).validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoExistsAndIsWellFormed() {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(true);

        Mockito.when(bagItMetadataReader.getBag(Mockito.any()))
            .thenReturn(Optional.of(new Bag()));

        assertDoesNotThrow(() -> checker.bagInfoExistsAndIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoDoesNotExist() {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(false);

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoExistsAndIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoDoesExistButItCouldNotBeOpened() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(true);

        Mockito.when(bagItMetadataReader.getBag(Mockito.any()))
            .thenReturn(Optional.empty());

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoExistsAndIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoCreatedElementIsIso8601Date() throws Exception {

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Created")))
            .thenReturn(List.of("2022-01-01T01:23:45.678+00:00"));

        assertDoesNotThrow(() -> checker.bagInfoCreatedElementIsIso8601Date().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoCreatedElementIsNotAValidDate() throws Exception {

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Created")))
            .thenReturn(List.of("2022-01-01 01:23:45.678"));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoCreatedElementIsIso8601Date().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsExactlyOneOf() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value"));

        assertDoesNotThrow(() -> checker.bagInfoContainsExactlyOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsExactlyOneOfButInRealityItIsTwo() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value", "secondvalue"));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoContainsExactlyOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsExactlyOneOfButInRealityItIsZero() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(new ArrayList<>());

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoContainsExactlyOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsAtMostOne() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value"));

        assertDoesNotThrow(() -> checker.bagInfoContainsAtMostOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsAtMostOneButItReturnsTwo() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value", "secondvalue"));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoContainsAtMostOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsAtMostOneOfButInRealityItIsZero() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(new ArrayList<>());

        assertDoesNotThrow(() -> checker.bagInfoContainsAtMostOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagShaPayloadManifestContainsAllPayloadFiles() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.getAllFiles(Mockito.any()))
            .thenReturn(List.of(Path.of("path/1.txt"), Path.of("path/2.txt")));

        var bag = new Bag();
        var manifest = new Manifest(StandardSupportedAlgorithms.SHA1);
        manifest.setFileToChecksumMap(Map.of(
            Path.of("path/2.txt"), "checksum2",
            Path.of("path/1.txt"), "checksum1"
        ));

        bag.setPayLoadManifests(Set.of(manifest));

        Mockito.when(bagItMetadataReader.getBag(Mockito.any())).thenReturn(Optional.of(bag));
        Mockito.when(bagItMetadataReader.getBagManifest(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(manifest));

        assertDoesNotThrow(() -> checker.bagShaPayloadManifestContainsAllPayloadFiles().validate(Path.of("bagdir")));
    }

    @Test
    void bagShaPayloadManifestMissesSomeFiles() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.getAllFiles(Mockito.any()))
            .thenReturn(List.of(Path.of("path/1.txt"), Path.of("path/2.txt"), Path.of("path/3.txt")));

        var bag = new Bag();
        var manifest = new Manifest(StandardSupportedAlgorithms.SHA1);
        manifest.setFileToChecksumMap(Map.of(
            Path.of("path/1.txt"), "checksum1",
            Path.of("path/2.txt"), "checksum2"
        ));

        bag.setPayLoadManifests(Set.of(manifest));

        Mockito.when(bagItMetadataReader.getBag(Mockito.any())).thenReturn(Optional.of(bag));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagShaPayloadManifestContainsAllPayloadFiles().validate(Path.of("bagdir")));
    }

    @Test
    void bagShaPayloadManifestHasTooManyFiles() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        Mockito.when(fileService.getAllFiles(Mockito.any()))
            .thenReturn(List.of(Path.of("path/1.txt"), Path.of("path/2.txt")));

        var bag = new Bag();
        var manifest = new Manifest(StandardSupportedAlgorithms.SHA1);
        manifest.setFileToChecksumMap(Map.of(
            Path.of("path/1.txt"), "checksum1",
            Path.of("path/2.txt"), "checksum2",
            Path.of("path/3.txt"), "checksum3"
        ));

        bag.setPayLoadManifests(Set.of(manifest));

        Mockito.when(bagItMetadataReader.getBag(Mockito.any())).thenReturn(Optional.of(bag));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagShaPayloadManifestContainsAllPayloadFiles().validate(Path.of("bagdir")));
    }

    @Test
    void containsNothingElseThan() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);
        var basePath = Path.of("bagdir/metadata");

        Mockito.when(fileService.getAllFilesAndDirectories(Mockito.eq(basePath)))
            .thenReturn(List.of(basePath.resolve("1.txt"), basePath.resolve("2.txt")));

        assertDoesNotThrow(() -> {
            checker.containsNothingElseThan(Path.of("metadata"), new String[] {
                "1.txt",
                "2.txt",
                "3.txt"
            }).validate(Path.of("bagdir"));
        });
    }

    @Test
    void containsNothingElseThanButThereAreInvalidFiles() throws Exception {
        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, bagXmlReader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var basePath = Path.of("bagdir/metadata");

        Mockito.when(fileService.getAllFilesAndDirectories(Mockito.eq(basePath)))
            .thenReturn(List.of(basePath.resolve("1.txt"), basePath.resolve("2.txt"), basePath.resolve("oh no.txt")));

        assertThrows(RuleViolationDetailsException.class, () -> {
            checker.containsNothingElseThan(Path.of("metadata"), new String[] {
                "1.txt"
                ,
                "2.txt",
                "3.txt"
            }).validate(Path.of("bagdir"));
        });
    }

    private Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new BagXmlReaderImpl().readXmlString(str);
    }

    @Test
    void ddmContainsUrnNbnIdentifier() throws ParserConfigurationException, IOException, SAXException {
        final String validDocument = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:URN\">urn:nbn:nl:ui:blabla</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(validDocument);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> {
            checker.ddmContainsUrnNbnIdentifier().validate(Path.of("bagdir"));
        });
    }

    @Test
    void ddmNotContainsUrnNbnIdentifier() throws ParserConfigurationException, IOException, SAXException {
        final String xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class, () -> {
            checker.ddmContainsUrnNbnIdentifier().validate(Path.of("bagdir"));
        });
    }

    @Test
    void ddmContainsInvalidURNIdentifier() throws ParserConfigurationException, IOException, SAXException {
        final String xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:URN\">invalid_urn</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class, () -> {
            checker.ddmContainsUrnNbnIdentifier().validate(Path.of("bagdir"));
        });
    }

    @Test
    void ddmDoiIdentifiersAreValid() throws Exception {
        final String xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.ddmDoiIdentifiersAreValid().validate(Path.of("bagdir")));
    }

    @Test
    void ddmDoiIdentifiersAreNotValid() throws Exception {
        final String xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">11.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">11.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class, () -> checker.ddmDoiIdentifiersAreValid().validate(Path.of("bagdir")));
    }

    @Test
    void ddmDaisAreValid() throws Exception {
        final String xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:profile>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:author>\n"
            + "                <dcx-dai:DAI>123456789</dcx-dai:DAI>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:DAI>info:eu-repo/dai/nl/298814064</dcx-dai:DAI>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.ddmDaisAreValid().validate(Path.of("bagdir")));
    }

    @Test
    void ddmDaisAreInvalid() throws Exception {
        final String xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:profile>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:author>\n"
            + "                <dcx-dai:DAI>123456788</dcx-dai:DAI>\n"
            + "                <dcx-dai:organization>\n"
            + "                    <dcx-dai:DAI>info:eu-repo/dai/nl/298814063</dcx-dai:DAI>\n"
            + "                </dcx-dai:organization>\n"
            + "            </dcx-dai:author>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class, () -> checker.ddmDaisAreValid().validate(Path.of("bagdir")));
    }

    @Test
    void ddmGmlPolygonPosListIsWellFormed() throws Exception {
        var xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "          <dcx-gml:spatial>\n"
            + "            <MultiSurface xmlns=\"http://www.opengis.net/gml\">\n"
            + "                <name>A random surface with multiple polygons</name>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon>\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                        <exterior>\n"
            + "                            <LinearRing>\n"
            + "                                <posList>52.079710 4.342778 52.079710 4.342778 52.07913 4.34332 52.079710 4.342778</posList>\n"
            + "                            </LinearRing>\n"
            + "                        </exterior>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon>\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                        <exterior>\n"
            + "                            <LinearRing>\n"
            + "                                <posList>52.079710 4.342778 52.079710 4.342778 52.07913 4.34332 52.079710 4.342778</posList>\n"
            + "                            </LinearRing>\n"
            + "                        </exterior>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "            </MultiSurface>\n"
            + "\t</dcx-gml:spatial>"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.ddmGmlPolygonPosListIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void ddmGmlPolygonPosListIsNotWellFormed() throws Exception {
        var xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "          <dcx-gml:spatial>\n"
            + "            <MultiSurface xmlns=\"http://www.opengis.net/gml\">\n"
            + "                <name>A random surface with multiple polygons</name>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon>\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                        <exterior>\n"
            + "                            <LinearRing>\n"
            + "                                <posList>52.079710 4.342778 52.079710 4.342778 52.079710 4.342778</posList>\n"
            + "                            </LinearRing>\n"
            + "                        </exterior>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon>\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                        <exterior>\n"
            + "                            <LinearRing>\n"
            + "                                <posList>52.079710 4.342778 52.079710 4.342778 52.07913 4.34332 52.079710 4.342778</posList>\n"
            + "                            </LinearRing>\n"
            + "                        </exterior>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "            </MultiSurface>\n"
            + "\t</dcx-gml:spatial>"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class, () -> checker.ddmGmlPolygonPosListIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void polygonsInSameMultiSurfaceHaveSameSrsName() throws Exception {
        var xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "          <dcx-gml:spatial>\n"
            + "            <MultiSurface xmlns=\"http://www.opengis.net/gml\">\n"
            + "                <name>A random surface with multiple polygons</name>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon srsName=\"http://google.com\">\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon srsName=\"http://google.com\">\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "            </MultiSurface>\n"
            + "\t</dcx-gml:spatial>"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.polygonsInSameMultiSurfaceHaveSameSrsName().validate(Path.of("bagdir")));
    }

    @Test
    void polygonsInSameMultiSurfaceHaveDifferentSrsName() throws Exception {
        var xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "          <dcx-gml:spatial>\n"
            + "            <MultiSurface xmlns=\"http://www.opengis.net/gml\">\n"
            + "                <name>A random surface with multiple polygons</name>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon srsName=\"http://yahoo.com\">\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon srsName=\"http://google.com\">\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "            </MultiSurface>\n"
            + "\t</dcx-gml:spatial>"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.polygonsInSameMultiSurfaceHaveSameSrsName().validate(Path.of("bagdir")));
    }

    @Test
    void polygonsInDifferentMultisurfacesHaveDifferentValuesButDontThrowAnException() throws Exception {
        var xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "          <dcx-gml:spatial>\n"
            + "            <MultiSurface xmlns=\"http://www.opengis.net/gml\">\n"
            + "                <name>A random surface with multiple polygons</name>\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon srsName=\"http://yahoo.com\">\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "            </MultiSurface>"
            + "            <MultiSurface xmlns=\"http://www.opengis.net/gml\">\n"
            + "                <surfaceMember>\n"
            + "                    <Polygon srsName=\"http://google.com\">\n"
            + "                        <description>A triangle between BP, De Horeca Academie en the railway station</description>\n"
            + "                    </Polygon>\n"
            + "\t\t        </surfaceMember>\n"
            + "            </MultiSurface>\n"
            + "\t</dcx-gml:spatial>"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.polygonsInSameMultiSurfaceHaveSameSrsName().validate(Path.of("bagdir")));
    }

    @Test
    void pointsHaveAtLeastTwoValues() throws Exception {
        var xml = "<ddm:DDM\n"
            + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "        xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "        xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
            + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "            <dct:spatial xsi:type=\"dcx-gml:SimpleGMLType\">\n"
            + "                <Point xmlns=\"http://www.opengis.net/gml\">\n"
            + "                    <pos>1.0</pos><!-- only one value -->\n"
            + "                </Point>\n"
            + "            </dct:spatial>\n"
            + "            <dcx-gml:spatial>\n"
            + "                <boundedBy xmlns=\"http://www.opengis.net/gml\">\n"
            + "                    <Envelope srsName=\"urn:ogc:def:crs:EPSG::28992\">\n"
            + "                        <lowerCorner>1 2</lowerCorner>\n"
            + "                        <upperCorner>1</upperCorner><!-- Only one value -->\n"
            + "                    </Envelope>\n"
            + "                </boundedBy>\n"
            + "            </dcx-gml:spatial>"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var exception = assertThrows(RuleViolationDetailsException.class,
            () -> checker.pointsHaveAtLeastTwoValues().validate(Path.of("bagdir")));
        assertTrue(exception.isMultiException());
        assertEquals(3, exception.getExceptions().size());
    }

    @Test
    void archisIdentifiersHaveAtMost10Characters() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
            + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">id1</dct:identifier>\n"
            + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">id2</dct:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.archisIdentifiersHaveAtMost10Characters().validate(Path.of("bagdir")));
    }

    @Test
    void archisIdentifiersHaveAtMost10CharactersButTheValuesAreLarger() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
            + "        <dct:rightsHolder>Mr. Rights</dct:rightsHolder>\n"
            + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">niet kunnen vinden1</dct:identifier>\n"
            + "        <dct:identifier xsi:type=\"id-type:ARCHIS-ZAAK-IDENTIFICATIE\">niet kunnen vinden2</dct:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var exceptions = assertThrows(RuleViolationDetailsException.class,
            () -> checker.archisIdentifiersHaveAtMost10Characters().validate(Path.of("bagdir")));

        assertEquals(2, exceptions.getExceptions().size());
        assertTrue(exceptions.getExceptions().get(0).getLocalizedMessage().contains("niet kunnen vinden1"));
        assertTrue(exceptions.getExceptions().get(1).getLocalizedMessage().contains("niet kunnen vinden2"));
    }

    @Test
    void allUrlsAreValid() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>PAN-00008136 - knobbed sickle</dc:title>\n"
            + "        <dcterms:description xml:lang=\"en\">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:organization>\n"
            + "                <dcx-dai:name xml:lang=\"en\">Portable Antiquities of the Netherlands</dcx-dai:name>\n"
            + "                <dcx-dai:role>DataCurator</dcx-dai:role>\n"
            + "            </dcx-dai:organization>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "        <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>\n"
            + "        <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>\n"
            + "        <ddm:audience>D37000</ddm:audience>\n"
            + "        <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>\n"
            + "    </ddm:profile>\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:spatial>Overbetuwe</dcterms:spatial>\n"
            + "        <dcterms:isFormatOf>PAN-00008136</dcterms:isFormatOf>\n"
            + "        <ddm:references href=\"https://www.portable-antiquities.nl/pan/#/object/public/8136\">Portable Antiquities of The Netherlands</ddm:references>\n"
            + "        <ddm:references scheme=\"URL\">http://abc.def</ddm:references>\n"
            + "        <ddm:references scheme=\"DOI\">10.17026/test-123-456</ddm:references>\n"
            + "        <ddm:references scheme=\"DOI\">https://dx.doi.org/doi:10.17026/test-123-456</ddm:references>\n"
            + "        <ddm:references scheme=\"DOI\" href=\"https://dx.doi.org/doi:10.17026/test-123-456\">a doi referencing my dataset</ddm:references>\n"
            + "        <ddm:references scheme=\"DOI\">http://doi.org/10.17026/test-123-456</ddm:references>\n"
            + "        <ddm:references scheme=\"URN\">urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66()+,-\\.:=@;$_!*'%/?#</ddm:references>\n"
            + "        <ddm:references scheme=\"id-type:URL\">http://abc.def</ddm:references>\n"
            + "        <ddm:references scheme=\"id-type:DOI\">10.17026/test-123-456</ddm:references>\n"
            + "        <ddm:references scheme=\"id-type:DOI\">https://dx.doi.org/doi:10.17026/test-123-456</ddm:references>\n"
            + "        <ddm:references scheme=\"id-type:DOI\" href=\"https://dx.doi.org/doi:10.17026/test-123-456\">a doi referencing my dataset</ddm:references>\n"
            + "        <ddm:references scheme=\"id-type:DOI\">http://doi.org/10.17026/test-123-456</ddm:references>\n"
            + "        <ddm:references scheme=\"id-type:URN\">urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66()+,-\\.:=@;$_!*'%/?#</ddm:references>\n"
            + "        <ddm:subject schemeURI=\"https://data.cultureelerfgoed.nl/term/id/pan/PAN\" subjectScheme=\"PAN thesaurus ideaaltypes\" valueURI=\"https://data.cultureelerfgoed.nl/term/id/pan/17-01-01\" xml:lang=\"en\">knobbed sickle</ddm:subject>\n"
            + "        <ddm:subject schemeURI=\"http://vocab.getty.edu/aat/\" subjectScheme=\"Art and Architecture Thesaurus\" valueURI=\"http://vocab.getty.edu/aat/300264860\" xml:lang=\"en\">Unknown</ddm:subject>\n"
            + "        <dc:subject>metaal</dc:subject>\n"
            + "        <dc:subject>koperlegering</dc:subject>\n"
            + "        <dcterms:identifier>PAN-00008136</dcterms:identifier>\n"
            + "        <dcterms:temporal xsi:type=\"abr:ABRperiode\">BRONSMB</dcterms:temporal>\n"
            + "        <dcterms:temporal xsi:type=\"abr:ABRperiode\">BRONSL</dcterms:temporal>\n"
            + "        <dcterms:temporal>-1500 until -800</dcterms:temporal>\n"
            + "        <dc:language xsi:type=\"dcterms:ISO639-2\">eng</dc:language>\n"
            + "        <dc:publisher xmlns:dc=\"http://purl.org/dc/terms/\">DANS/KNAW</dc:publisher>\n"
            + "        <dc:type xsi:type=\"dcterms:DCMIType\" xmlns:dc=\"http://purl.org/dc/terms/\">Dataset</dc:type>\n"
            + "        <dc:format xsi:type=\"dcterms:IMT\">image/jpeg</dc:format>\n"
            + "        <dc:format xsi:type=\"dcterms:IMT\">application/xml</dc:format>\n"
            + "        <dcterms:license xsi:type=\"dcterms:URI\">http://creativecommons.org/licenses/by-nc-sa/4.0/</dcterms:license>\n"
            + "        <dcterms:rightsHolder>Vrije Universiteit Amsterdam</dcterms:rightsHolder>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.allUrlsAreValid().validate(Path.of("bagdir")));
    }

    @Test
    void validateUrlsButSomeAreInvalid() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>PAN-00008136 - knobbed sickle</dc:title>\n"
            + "        <dcterms:description xml:lang=\"en\">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:organization>\n"
            + "                <dcx-dai:name xml:lang=\"en\">Portable Antiquities of the Netherlands</dcx-dai:name>\n"
            + "                <dcx-dai:role>DataCurator</dcx-dai:role>\n"
            + "            </dcx-dai:organization>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "        <ddm:created>2017-10-23T17:06:11+02:00</ddm:created>\n"
            + "        <ddm:available>2017-10-23T17:06:11+02:00</ddm:available>\n"
            + "        <ddm:audience>D37000</ddm:audience>\n"
            + "        <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>\n"
            + "    </ddm:profile>\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dcterms:spatial>Overbetuwe</dcterms:spatial>\n"
            + "        <dcterms:isFormatOf>PAN-00008136</dcterms:isFormatOf>\n"
            + "        <ddm:references href=\"xttps://www.portable-antiquities.nl/pan/#/object/public/8136\">Portable Antiquities of The Netherlands</ddm:references>\n"
            + "        <ddm:references scheme=\"URL\">xttp://abc.def</ddm:references>\n"
            + "        <ddm:references scheme=\"DOI\">99.1234.abc</ddm:references>\n"
            + "        <ddm:references scheme=\"URN\">uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66</ddm:references>\n"
            + "        <ddm:isFormatOf scheme=\"id-type:DOI\">joopajoo</ddm:isFormatOf>\n"
            + "        <ddm:isFormatOf scheme=\"id-type:URN\">niinpä</ddm:isFormatOf>\n"
            + "        <ddm:subject schemeURI=\"xttps://data.cultureelerfgoed.nl/term/id/pan/PAN\" subjectScheme=\"PAN thesaurus ideaaltypes\" valueURI=\"xttps://data.cultureelerfgoed.nl/term/id/pan/17-01-01\" xml:lang=\"en\">knobbed sickle</ddm:subject>\n"
            + "        <ddm:subject schemeURI=\"http://vocab.getty.edu/aat/\" subjectScheme=\"Art and Architecture Thesaurus\" valueURI=\"http://vocab.getty.edu/aat/300264860\" xml:lang=\"en\">Unknown</ddm:subject>\n"
            + "        <dc:subject>metaal</dc:subject>\n"
            + "        <dc:subject>koperlegering</dc:subject>\n"
            + "        <dcterms:identifier>PAN-00008136</dcterms:identifier>\n"
            + "        <dcterms:temporal xsi:type=\"abr:ABRperiode\">BRONSMB</dcterms:temporal>\n"
            + "        <dcterms:temporal xsi:type=\"abr:ABRperiode\">BRONSL</dcterms:temporal>\n"
            + "        <dcterms:temporal>-1500 until -800</dcterms:temporal>\n"
            + "        <dc:language xsi:type=\"dcterms:ISO639-2\">eng</dc:language>\n"
            + "        <dc:publisher xmlns:dc=\"http://purl.org/dc/terms/\">DANS/KNAW</dc:publisher>\n"
            + "        <dc:type xsi:type=\"dcterms:DCMIType\" xmlns:dc=\"http://purl.org/dc/terms/\">Dataset</dc:type>\n"
            + "        <dc:format xsi:type=\"dcterms:IMT\">image/jpeg</dc:format>\n"
            + "        <dc:format xsi:type=\"dcterms:IMT\">application/xml</dc:format>\n"
            + "        <dcterms:license xsi:type=\"dcterms:URI\">ettp://creativecommons.org/licenses/by-nc-sa/4.0/</dcterms:license>\n"
            + "        <dcterms:rightsHolder>Vrije Universiteit Amsterdam</dcterms:rightsHolder>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234/fantasy-doi-id</dcterms:identifier>\n"
            + "        <dcterms:identifier xsi:type=\"id-type:DOI\">10.1234.567/issn-987-654</dcterms:identifier>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var exceptions = assertThrows(RuleViolationDetailsException.class,
            () -> checker.allUrlsAreValid().validate(Path.of("bagdir")));

        assertEquals(6, exceptions.getExceptions().size());
    }

    @Test
    void ddmMustHaveRightsHolder() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:author>\n"
            + "                <dcx-dai:role>Distributor</dcx-dai:role>\n"
            + "            </dcx-dai:author>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "        <ddm:accessRights>OPEN_ACCESS_FOR_REGISTERED_USERS</ddm:accessRights>\n"
            + "    </ddm:profile>\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
            + "        <dct:rightsHolder>Johny</dct:rightsHolder>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.ddmMustHaveRightsHolder().validate(Path.of("bagdir")));

    }

    @Test
    void ddmMustHaveRightsHolderButItDoesntExist() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:author>\n"
            + "                <dcx-dai:role>Distributor</dcx-dai:role>\n"
            + "            </dcx-dai:author>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "        <ddm:accessRights>OPEN_ACCESS_FOR_REGISTERED_USERS</ddm:accessRights>\n"
            + "    </ddm:profile>\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.ddmMustHaveRightsHolder().validate(Path.of("bagdir")));
    }

    @Test
    void xmlFileConfirmsToSchema() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dc:title>PAN-00008136 - knobbed sickle</dc:title>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());
        Mockito.doReturn(new ArrayList<SAXParseException>())
            .when(xmlValidator).validateDocument(Mockito.any(), Mockito.anyString());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.xmlFileConfirmsToSchema(Path.of("metadata/dataset.xml"), "ddm").validate(Path.of("bagdir")));
    }

    @Test
    void xmlFileDoesNotConformToSchema() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
            + "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\" xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:abr=\"http://www.den.nl/standaard/166/Archeologisch-Basisregister/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\" xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\" xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dcterms:description xml:lang=\"en\">This find is registered at Portable Antiquities of the Netherlands with number PAN-00008136</dcterms:description>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        Mockito.doReturn(List.of(new SAXParseException("msg", null)))
            .when(xmlValidator).validateDocument(Mockito.any(), Mockito.anyString());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.xmlFileConfirmsToSchema(Path.of("metadata/dataset.xml"), "ddm").validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlHasDocumentElementFiles() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlHasDocumentElementFiles().validate(Path.of("bagdir")));

    }

    @Test
    void filesXmlDoesNotHaveDocumentElementFiles() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<notfiles xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "</notfiles>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlHasDocumentElementFiles().validate(Path.of("bagdir")));

    }

    @Test
    void filesXmlHasOnlyFiles() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlHasOnlyFiles().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlHasMoreThanOnlyFiles() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <path></path>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlHasOnlyFiles().validate(Path.of("bagdir")));

        assertTrue(e.getLocalizedMessage().contains("path"));
    }

    @Test
    void filesXmlFileElementsAllHaveFilepathAttribute() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlFileElementsAllHaveFilepathAttribute().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlFileElementsAllHaveFilepathAttributeButNotALl() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file>\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlFileElementsAllHaveFilepathAttribute().validate(Path.of("bagdir")));

        assertTrue(e.getLocalizedMessage().startsWith("2"));
    }

    @Test
    void filesXmlNoDuplicatesAndMatchesWithPayloadPlusPreStagedFiles() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        var files = List.of(
            Path.of("bagdir/data/random images/image01.png"),
            Path.of("bagdir/data/random images/image02.png"),
            Path.of("bagdir/data/random images/image03.png")
        );
        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlNoDuplicatesAndMatchesWithPayloadPlusPreStagedFiles().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlNoDuplicatesAndMatchesWithPayloadPlusPreStagedFilesWithNamespace() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        var files = List.of(
            Path.of("bagdir/data/random images/image01.png"),
            Path.of("bagdir/data/random images/image02.png"),
            Path.of("bagdir/data/random images/image03.png")
        );
        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlNoDuplicatesAndMatchesWithPayloadPlusPreStagedFiles().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlNoDuplicatesAndMatchesWithPayloadPlusPreStagedFilesWithErrors() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());

        var files = List.of(
            Path.of("data/random images/image01.png"),
            Path.of("data/random images/image02.png"),
            Path.of("data/random images/image04.png")
        );
        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlNoDuplicatesAndMatchesWithPayloadPlusPreStagedFiles().validate(Path.of("bagdir")));

        assertTrue(e.getLocalizedMessage().contains("image03.png"));
        assertTrue(e.getLocalizedMessage().contains("image04.png"));
    }

    @Test
    void filesXmlAllFilesHaveFormat() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/a/deeper/path/With some file.txt\">\n"
            + "        <dcterms:format>text/plain</dcterms:format>\n"
            + "        <dcterms:created>2016-11-09</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";
        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlAllFilesHaveFormat().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlAllFilesHaveFormatButSomeDont() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.jpeg\">\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/a/deeper/path/With some file.txt\">\n"
            + "        <dcterms:format>text/plain</dcterms:format>\n"
            + "        <dcterms:created>2016-11-09</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";
        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlAllFilesHaveFormat().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlFilesHaveOnlyAllowedNamespaces() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:something=\"http://dans.knaw.nl/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.jpeg\">\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/a/deeper/path/With some file.txt\">\n"
            + "        <dcterms:format>text/plain</dcterms:format>\n"
            + "        <dcterms:created>2016-11-09</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";
        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlFilesHaveOnlyAllowedNamespaces().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlFilesHaveOnlyAllowedNamespacesButOneIsDifferent() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:something=\"http://dans.knaw.nl/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.jpeg\">\n"
            + "        <something:format>image/jpeg</something:format>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.jpeg\">\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/a/deeper/path/With some file.txt\">\n"
            + "        <dcterms:format>text/plain</dcterms:format>\n"
            + "        <dcterms:created>2016-11-09</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";
        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlFilesHaveOnlyAllowedNamespaces().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlFilesHaveOnlyAllowedAccessRights() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:accessRights>ANONYMOUS</dcterms:accessRights>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image03.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:accessRights>RESTRICTED_REQUEST</dcterms:accessRights>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image04.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:accessRights>NONE</dcterms:accessRights>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";
        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        assertDoesNotThrow(() -> checker.filesXmlFilesHaveOnlyAllowedAccessRights().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlFilesHaveOnlyAllowedAccessRightsButOneIsIncorrected() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.jpeg\">\n"
            + "        <dcterms:format>image/jpeg</dcterms:format>\n"
            + "        <dcterms:accessRights>WRONG_VALUE</dcterms:accessRights>\n"
            + "        <dcterms:created>2016-11-10</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";
        var document = parseXmlString(xml);
        var reader = Mockito.spy(new BagXmlReaderImpl());
        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagInfoCheckerImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, daiDigestCalculator, polygonListValidator, xmlValidator);

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlFilesHaveOnlyAllowedAccessRights().validate(Path.of("bagdir")));

        assertTrue(e.getExceptions().get(0).getLocalizedMessage().contains("WRONG_VALUE"));
    }
}
