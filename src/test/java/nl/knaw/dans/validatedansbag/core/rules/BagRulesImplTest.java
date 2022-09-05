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

import gov.loc.repository.bagit.domain.Bag;
import gov.loc.repository.bagit.domain.Manifest;
import gov.loc.repository.bagit.exceptions.InvalidBagitFileFormatException;
import gov.loc.repository.bagit.hash.StandardSupportedAlgorithms;
import nl.knaw.dans.validatedansbag.core.engine.RuleViolationDetailsException;
import nl.knaw.dans.validatedansbag.core.service.BagItMetadataReader;
import nl.knaw.dans.validatedansbag.core.service.DataverseService;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsService;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidator;
import nl.knaw.dans.validatedansbag.core.validator.IdentifierValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidator;
import nl.knaw.dans.validatedansbag.core.validator.LicenseValidatorImpl;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidator;
import nl.knaw.dans.validatedansbag.core.validator.PolygonListValidatorImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BagRulesImplTest {

    final FileService fileService = Mockito.mock(FileService.class);
    final XmlReader xmlReader = Mockito.mock(XmlReader.class);
    final IdentifierValidator identifierValidator = new IdentifierValidatorImpl();
    final BagItMetadataReader bagItMetadataReader = Mockito.mock(BagItMetadataReader.class);
    final PolygonListValidator polygonListValidator = new PolygonListValidatorImpl();
    final OriginalFilepathsService originalFilepathsService = Mockito.mock(OriginalFilepathsService.class);
    final DataverseService dataverseService = Mockito.mock(DataverseService.class);

    final LicenseValidator licenseValidator = new LicenseValidatorImpl(new TestLicenseConfig());

    @AfterEach
    void afterEach() {
        Mockito.reset(fileService);
        Mockito.reset(xmlReader);
        Mockito.reset(bagItMetadataReader);
        Mockito.reset(dataverseService);
        Mockito.reset(originalFilepathsService);
    }

    @Test
    void testBagIsValid() throws Exception {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertDoesNotThrow(() -> checker.bagIsValid().validate(Path.of("testpath")));

        Mockito.verify(bagItMetadataReader).verifyBag(Path.of("testpath"));

    }

    @Test
    void testBagIsNotValidWithExceptionThrown() throws Exception {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.doThrow(new InvalidBagitFileFormatException("Invalid file format"))
            .when(bagItMetadataReader).verifyBag(Mockito.any());

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagIsValid().validate(Path.of("testpath")));
    }

    @Test
    void containsDirWorks() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isDirectory(Mockito.any()))
            .thenReturn(true);

        assertDoesNotThrow(() -> checker.containsDir(Path.of("testpath")).validate(Path.of("bagdir")));

        Mockito.verify(fileService).isDirectory(Path.of("bagdir/testpath"));
    }

    @Test
    void containsDirThrowsException() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isDirectory(Mockito.any()))
            .thenReturn(false);

        assertThrows(RuleViolationDetailsException.class, () -> checker.containsDir(Path.of("testpath")).validate(Path.of("bagdir")));
    }

    @Test
    void containsFileWorks() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(true);

        assertDoesNotThrow(() -> checker.containsFile(Path.of("testpath")).validate(Path.of("bagdir")));

        Mockito.verify(fileService).isFile(Path.of("bagdir/testpath"));
    }

    @Test
    void containsFileThrowsException() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(false);

        assertThrows(RuleViolationDetailsException.class, () -> checker.containsFile(Path.of("testpath")).validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoExistsAndIsWellFormed() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(true);

        Mockito.when(bagItMetadataReader.getBag(Mockito.any()))
            .thenReturn(Optional.of(new Bag()));

        assertDoesNotThrow(() -> checker.bagInfoExistsAndIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoDoesNotExist() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(false);

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoExistsAndIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoDoesExistButItCouldNotBeOpened() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(fileService.isFile(Mockito.any()))
            .thenReturn(true);

        Mockito.when(bagItMetadataReader.getBag(Mockito.any()))
            .thenReturn(Optional.empty());

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoExistsAndIsWellFormed().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoCreatedElementIsIso8601Date() {

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(bagItMetadataReader.getSingleField(Mockito.any(), Mockito.eq("Created")))
            .thenReturn("2022-01-01T01:23:45.678+00:00");

        assertDoesNotThrow(() -> checker.bagInfoCreatedElementIsIso8601Date().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoCreatedElementIsNotAValidDate() {

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Created")))
            .thenReturn(List.of("2022-01-01 01:23:45.678"))
            .thenReturn(List.of("2022-01-01 01:23:45+00:00"));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoCreatedElementIsIso8601Date().validate(Path.of("bagdir")));
        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoCreatedElementIsIso8601Date().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsExactlyOneOf() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value"));

        assertDoesNotThrow(() -> checker.bagInfoContainsExactlyOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsExactlyOneOfButInRealityItIsTwo() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value", "secondvalue"));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoContainsExactlyOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsExactlyOneOfButInRealityItIsZero() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(new ArrayList<>());

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoContainsExactlyOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsAtMostOne() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value"));

        assertDoesNotThrow(() -> checker.bagInfoContainsAtMostOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoIsVersionOfIsValidUrnUuid() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Is-Version-Of")))
            .thenReturn(List.of("urn:uuid:76cfdebf-e43d-4c56-a886-e8375c745429"));

        assertDoesNotThrow(() -> checker.bagInfoIsVersionOfIsValidUrnUuid().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoIsVersionOfIsNotValidUrnUuid() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Is-Version-Of")))
            .thenReturn(List.of("http://google.com"))
            .thenReturn(List.of("urn:uuid:1234"))
            .thenReturn(List.of("urn:not uuid:1234"));

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.bagInfoIsVersionOfIsValidUrnUuid().validate(Path.of("bagdir")));
        assertThrows(RuleViolationDetailsException.class,
            () -> checker.bagInfoIsVersionOfIsValidUrnUuid().validate(Path.of("bagdir")));
        assertThrows(RuleViolationDetailsException.class,
            () -> checker.bagInfoIsVersionOfIsValidUrnUuid().validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsAtMostOneButItReturnsTwo() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(List.of("value", "secondvalue"));

        assertThrows(RuleViolationDetailsException.class, () -> checker.bagInfoContainsAtMostOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagInfoContainsAtMostOneOfButInRealityItIsZero() {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
            .thenReturn(new ArrayList<>());

        assertDoesNotThrow(() -> checker.bagInfoContainsAtMostOneOf("Key").validate(Path.of("bagdir")));
    }

    @Test
    void bagShaPayloadManifestContainsAllPayloadFiles() throws Exception {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);
        var basePath = Path.of("bagdir/metadata");

        Mockito.when(fileService.getAllFilesAndDirectories(Mockito.eq(basePath)))
            .thenReturn(List.of(basePath.resolve("1.txt"), basePath.resolve("2.txt")));

        assertDoesNotThrow(() -> checker.containsNothingElseThan(Path.of("metadata"), new String[] {
            "1.txt",
            "2.txt",
            "3.txt"
        }).validate(Path.of("bagdir")));
    }

    @Test
    void containsNothingElseThanButThereAreInvalidFiles() throws Exception {
        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        var basePath = Path.of("bagdir/metadata");

        Mockito.when(fileService.getAllFilesAndDirectories(Mockito.eq(basePath)))
            .thenReturn(List.of(basePath.resolve("1.txt"), basePath.resolve("2.txt"), basePath.resolve("oh no.txt")));

        assertThrows(RuleViolationDetailsException.class, () -> checker.containsNothingElseThan(Path.of("metadata"), new String[] {
            "1.txt"
            ,
            "2.txt",
            "3.txt"
        }).validate(Path.of("bagdir")));
    }

    private Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(str);
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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
            + "                <dcx-dai:ISNI>http://www.isni.org/isni/0000000114559647</dcx-dai:ISNI>\n"
            + "                <dcx-dai:ORCID>http://orcid.org/0000-0002-1825-0097</dcx-dai:ORCID>\n"
            + "            </dcx-dai:author>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "    </ddm:profile>\n"
            + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertDoesNotThrow(() -> checker.ddmDaisAreValid().validate(Path.of("bagdir")));
        assertDoesNotThrow(() -> checker.ddmOrcidsAreValid().validate(Path.of("bagdir")));
        assertDoesNotThrow(() -> checker.ddmIsnisAreValid().validate(Path.of("bagdir")));
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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        var exceptions = assertThrows(RuleViolationDetailsException.class,
            () -> checker.allUrlsAreValid().validate(Path.of("bagdir")));

        assertEquals(4, exceptions.getExceptions().size());
    }

    @Test
    void ddmMustHaveRightsHolderDeposit() throws Exception {
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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertDoesNotThrow(() -> checker.ddmMustHaveRightsHolderDeposit().validate(Path.of("bagdir")));
    }

    @Test
    void ddmMustHaveRightsHolderDepositButItDoesntExist() throws Exception {
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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.ddmMustHaveRightsHolderDeposit().validate(Path.of("bagdir")));
    }

    @Test
    void ddmMustHaveRightsHolderMigration() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://easy.dans.knaw.nl/schemas/md/ddm/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
            + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
            + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
            + "         xsi:schemaLocation=\"http://easy.dans.knaw.nl/schemas/md/ddm/ http://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd\">\n"
            + "    <ddm:profile>\n"
            + "        <dcx-dai:creatorDetails>\n"
            + "            <dcx-dai:author>\n"
            + "                <dcx-dai:role>RightsHolder</dcx-dai:role>\n"
            + "            </dcx-dai:author>\n"
            + "        </dcx-dai:creatorDetails>\n"
            + "        <ddm:accessRights>OPEN_ACCESS_FOR_REGISTERED_USERS</ddm:accessRights>\n"
            + "    </ddm:profile>\n"
            + "    <ddm:dcmiMetadata>\n"
            + "        <dct:license xsi:type=\"dct:URI\">http://creativecommons.org/licenses/by-sa/4.0</dct:license>\n"
            + "    </ddm:dcmiMetadata>\n"
            + "</ddm:DDM>\n";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertDoesNotThrow(() -> checker.ddmMustHaveRightsHolderMigration().validate(Path.of("bagdir")));
    }

    @Test
    void ddmMustHaveRightsHolderMigrationButItDoesntExist() throws Exception {
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
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, reader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertThrows(RuleViolationDetailsException.class,
            () -> checker.ddmMustHaveRightsHolderMigration().validate(Path.of("bagdir")));
    }

    @Test
    void originalFilePathsDoNotContainSpaces() {

        var mapping = List.of(
            new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/some with spaces.txt"), Path.of("data/12345")),
            new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/some with spaces2.txt"), Path.of("data/filename.txt")),
            new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/some with spaces3.txt"), Path.of("data/dlsckjdfg"))
        );

        Mockito.doReturn(mapping).when(originalFilepathsService).getMapping(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        assertDoesNotThrow(() ->
            checker.originalFilePathsDoNotContainSpaces().validate(Path.of("bagdir")));
    }

    @Test
    void originalFilePathsDoNotContainSpacesButItActuallyDoes() {

        var mapping = List.of(
            new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/some with spaces.txt"), Path.of("data/12345 spaces.txt")),
            new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/some with spaces2.txt"), Path.of("data/filename.txt")),
            new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/some with spaces3.txt"), Path.of("data dir/xyz abc"))
        );

        Mockito.doReturn(mapping).when(originalFilepathsService).getMapping(Mockito.any());

        var checker = new BagRulesImpl(fileService, bagItMetadataReader, xmlReader, originalFilepathsService, identifierValidator, polygonListValidator, licenseValidator);

        var exception = assertThrows(RuleViolationDetailsException.class, () ->
            checker.originalFilePathsDoNotContainSpaces().validate(Path.of("bagdir")));

        assertTrue(exception.getMessage().contains("12345 spaces.txt"));
        assertTrue(exception.getMessage().contains("data dir/xyz abc"));
        assertFalse(exception.getMessage().contains("filename.txt"));
    }
}
