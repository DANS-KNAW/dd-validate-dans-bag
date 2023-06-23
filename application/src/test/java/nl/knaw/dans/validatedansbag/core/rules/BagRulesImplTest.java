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
import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.engine.RuleResult.Status;
import nl.knaw.dans.validatedansbag.core.service.*;
import nl.knaw.dans.validatedansbag.core.validator.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BagRulesImplTest {
    final FileService fileService = Mockito.mock(FileService.class);
    final XmlReader xmlReader = Mockito.mock(XmlReader.class);
    final IdentifierValidator identifierValidator = new IdentifierValidatorImpl();
    final BagItMetadataReader bagItMetadataReader = Mockito.mock(BagItMetadataReader.class);
    final PolygonListValidator polygonListValidator = new PolygonListValidatorImpl();
    final OriginalFilepathsService originalFilepathsService = Mockito.mock(OriginalFilepathsService.class);
    final DataverseService dataverseService = Mockito.mock(DataverseService.class);

    final LicenseValidator licenseValidator = new LicenseValidatorImpl(dataverseService);
    final FilesXmlService filesXmlService = Mockito.mock(FilesXmlService.class);

    final OrganizationIdentifierPrefixValidator organizationIdentifierPrefixValidator = new OrganizationIdentifierPrefixValidatorImpl(
            List.of("USER1-", "U2:")
    );

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
        var result = new BagIsValid(bagItMetadataReader).validate(Path.of("testpath"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());

        Mockito.verify(bagItMetadataReader).verifyBag(Path.of("testpath"));
    }

    @Test
    void testBagIsNotValidWithExceptionThrown() throws Exception {
        Mockito.doThrow(new InvalidBagitFileFormatException("Invalid file format"))
                .when(bagItMetadataReader).verifyBag(Mockito.any());

        var result = new BagIsValid(bagItMetadataReader).validate(Path.of("testpath"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void containsDirWorks() throws Exception {
        Mockito.when(fileService.isDirectory(Mockito.any()))
                .thenReturn(true);

        var result = new ContainsDir(Path.of("testpath"), fileService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
        Mockito.verify(fileService).isDirectory(Path.of("bagdir/testpath"));
    }

    @Test
    void containsDirThrowsException() throws Exception {
        Mockito.when(fileService.isDirectory(Mockito.any()))
                .thenReturn(false);

        var result = new ContainsDir(Path.of("testpath"), fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void containsFileWorks() throws Exception {
        Mockito.when(fileService.isFile(Mockito.any()))
                .thenReturn(true);

        var result = new ContainsFile(Path.of("testpath"), fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());

        Mockito.verify(fileService).isFile(Path.of("bagdir/testpath"));
    }

    @Test
    void containsFileThrowsException() throws Exception {
        Mockito.when(fileService.isFile(Mockito.any()))
                .thenReturn(false);

        var result = new ContainsFile(Path.of("testpath"), fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoExistsAndIsWellFormed() throws Exception {
        Mockito.when(fileService.isFile(Mockito.any()))
                .thenReturn(true);

        Mockito.when(bagItMetadataReader.getBag(Mockito.any()))
                .thenReturn(Optional.of(new Bag()));

        var result = new BagInfoExistsAndIsWellformed(bagItMetadataReader, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void bagInfoDoesNotExist() throws Exception {
        Mockito.when(fileService.isFile(Mockito.any()))
                .thenReturn(false);

        var result = new BagInfoExistsAndIsWellformed(bagItMetadataReader, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoDoesExistButItCouldNotBeOpened() throws Exception {
        Mockito.when(fileService.isFile(Mockito.any()))
                .thenReturn(true);

        Mockito.when(bagItMetadataReader.getBag(Mockito.any()))
                .thenReturn(Optional.empty());

        var result = new BagInfoExistsAndIsWellformed(bagItMetadataReader, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoCreatedElementIsIso8601Date() throws Exception {
        Mockito.when(bagItMetadataReader.getSingleField(Mockito.any(), Mockito.eq("Created")))
                .thenReturn("2022-01-01T01:23:45.678+00:00");

        var result = new BagInfoCreatedElementIsIso8601Date(bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void bagInfoCreatedElementIsNotAValidDate() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Created")))
                .thenReturn(List.of("2022-01-01 01:23:45.678"))
                .thenReturn(List.of("2022-01-01 01:23:45+00:00"));

        var result = new BagInfoCreatedElementIsIso8601Date(bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoContainsExactlyOneOf() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
                .thenReturn(List.of("value"));

        var result = new BagInfoContainsExactlyOneOf("Key", bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void bagInfoContainsExactlyOneOfButInRealityItIsTwo() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
                .thenReturn(List.of("value", "secondvalue"));

        var result = new BagInfoContainsExactlyOneOf("Key", bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoContainsExactlyOneOfButInRealityItIsZero() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
                .thenReturn(new ArrayList<>());

        var result = new BagInfoContainsExactlyOneOf("Key", bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoContainsAtMostOne() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
                .thenReturn(List.of("value"));

        var result = new BagInfoContainsAtMostOneOf("Key", bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void bagInfoIsVersionOfIsValidUrnUuid() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Is-Version-Of")))
                .thenReturn(List.of("urn:uuid:76cfdebf-e43d-4c56-a886-e8375c745429"));

        var result = new BagInfoIsVersionOfIsValidUrnUuid(bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void bagInfoIsVersionOfIsNotValidUrnUuid() throws Exception {
        // TODO: refactor to proper parameterized test
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Is-Version-Of")))
                .thenReturn(List.of("http://google.com"))
                .thenReturn(List.of("urn:uuid:1234"))
                .thenReturn(List.of("urn:notuuid:1234"));

        assertEquals(RuleResult.Status.ERROR, new BagInfoIsVersionOfIsValidUrnUuid(bagItMetadataReader).validate(Path.of("bagdir")).getStatus());
        assertEquals(RuleResult.Status.ERROR, new BagInfoIsVersionOfIsValidUrnUuid(bagItMetadataReader).validate(Path.of("bagdir")).getStatus());
        assertEquals(RuleResult.Status.ERROR, new BagInfoIsVersionOfIsValidUrnUuid(bagItMetadataReader).validate(Path.of("bagdir")).getStatus());
    }

    @Test
    void bagInfoContainsAtMostOneButItReturnsTwo() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
                .thenReturn(List.of("value", "secondvalue"));

        var result = new BagInfoContainsAtMostOneOf("Key", bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void bagInfoContainsAtMostOneOfButInRealityItIsZero() throws Exception {
        Mockito.when(bagItMetadataReader.getField(Mockito.any(), Mockito.eq("Key")))
                .thenReturn(new ArrayList<>());

        var result = new BagInfoContainsAtMostOneOf("Key", bagItMetadataReader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SKIP_DEPENDENCIES, result.getStatus());
    }

    @Test
    void containsNothingElseThan() throws Exception {
        var basePath = Path.of("bagdir/metadata");

        Mockito.when(fileService.getAllFilesAndDirectories(Mockito.eq(basePath)))
                .thenReturn(List.of(basePath.resolve("1.txt"), basePath.resolve("2.txt")));

        var result = new ContainsNothingElseThan(Path.of("metadata"), new String[]{
                "1.txt",
                "2.txt",
                "3.txt"
        }, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void containsNothingElseThanButThereAreInvalidFiles() throws Exception {
        var basePath = Path.of("bagdir/metadata");

        Mockito.when(fileService.getAllFilesAndDirectories(Mockito.eq(basePath)))
                .thenReturn(List.of(basePath.resolve("1.txt"), basePath.resolve("2.txt"), basePath.resolve("oh no.txt")));

        var result = new ContainsNothingElseThan(Path.of("metadata"), new String[]{
                "1.txt",
                "2.txt",
                "3.txt"
        }, fileService).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    private Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(str);
    }

    @Test
    void ddmDaisAreValid() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        assertEquals(RuleResult.Status.SUCCESS, new DdmDaisAreValid(reader, identifierValidator).validate(Path.of("bagdir")).getStatus());
        assertEquals(RuleResult.Status.SUCCESS, new DdmDaisAreValid(reader, identifierValidator).validate(Path.of("bagdir")).getStatus());
        assertEquals(RuleResult.Status.SUCCESS, new DdmDaisAreValid(reader, identifierValidator).validate(Path.of("bagdir")).getStatus());
    }

    @Test
    void ddmDaisAreInvalid() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        var result = new DdmDaisAreValid(reader, identifierValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void ddmMustContainDctermsLicense_should_return_ERROR_for_zero_licenses() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DdmMustContainExactlyOneDctermsLicenseWithXsiTypeUri(reader, licenseValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void ddmMustContainDctermsLicense_should_return_ERROR_for_multiple_licenses() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dcterms:license xsi:type=\"dcterms:URI\">http://opensource.org/licenses/MIT</dcterms:license>\n"
                + "        <dcterms:license xsi:type=\"dcterms:URI\">http://opensource.org/licenses/MIT</dcterms:license>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DdmMustContainExactlyOneDctermsLicenseWithXsiTypeUri(reader, licenseValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void ddmMustContainDctermsLicense_should_return_SUCCESS_for_valid_URI() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dcterms:license xsi:type=\"dcterms:URI\">http://random.org/licenses/MIT</dcterms:license>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DdmMustContainExactlyOneDctermsLicenseWithXsiTypeUri(reader, licenseValidator).validate(Path.of("bagdir"));
        assertEquals(Status.SUCCESS, result.getStatus());
    }

    @Test
    void ddmMustContainDctermsLicense_should_return_ERROR_for_nondcterms_uri() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dcterms:license xsi:type=\"dcterms:SOMETHING_ELSE\">http://random.org/licenses/MIT</dcterms:license>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DdmMustContainExactlyOneDctermsLicenseWithXsiTypeUri(reader, licenseValidator).validate(Path.of("bagdir"));
        assertEquals(Status.ERROR, result.getStatus());
    }

    @Test
    void ddmMustContainDctermsLicense_should_return_ERROR_for_invalid_uri() throws Exception {
        final String xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "        <dcterms:license xsi:type=\"dcterms:URI\">invalid uri</dcterms:license>\n"
                + "    </ddm:dcmiMetadata>\n"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DdmMustContainExactlyOneDctermsLicenseWithXsiTypeUri(reader, licenseValidator).validate(Path.of("bagdir"));

        assertEquals(Status.ERROR, result.getStatus());
    }

    @Test
    void ddmGmlPolygonPosListIsWellFormed() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        var result = new DdmGmlPolygonPosListIsWellFormed(reader, polygonListValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void ddmGmlPolygonPosListIsNotWellFormed() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        var result = new DdmGmlPolygonPosListIsWellFormed(reader, polygonListValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void polygonsInSameMultiSurfaceHaveSameSrsName() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        var result = new PolygonsInSameMultiSurfaceHaveSameSrsName(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void polygonsInSameMultiSurfaceHaveDifferentSrsName() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        var result = new PolygonsInSameMultiSurfaceHaveSameSrsName(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void polygonsInDifferentMultisurfacesHaveDifferentValuesButDontThrowAnException() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
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

        var result = new PolygonsInSameMultiSurfaceHaveSameSrsName(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void pointsShouldHaveAtLeastTwoNumericValuesWithinBounds() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "            <dct:spatial xsi:type='dcx-gml:SimpleGMLType'>"
                + "                <Point xmlns='http://www.opengis.net/gml'>"
                + "                    <pos>1.0</pos><!-- only one value -->"
                + "                </Point>"
                + "            </dct:spatial>"
                + "            <dct:spatial xsi:type='dcx-gml:SimpleGMLType'>"
                + "                <Point xmlns='http://www.opengis.net/gml'>"
                + "                    <pos>a 5</pos><!-- non numeric -->"
                + "                </Point>"
                + "            </dct:spatial>"
                + "            <dcx-gml:spatial>"
                + "                <boundedBy xmlns='http://www.opengis.net/gml'>"
                + "                    <Envelope srsName='urn:ogc:def:crs:EPSG::28992'>"
                + "                        <lowerCorner>-7001 289000</lowerCorner>"
                + "                        <upperCorner>300001 629000</upperCorner>"
                + "                    </Envelope>"
                + "                </boundedBy>"
                + "            </dcx-gml:spatial>"
                + "            <dcx-gml:spatial>"
                + "                <boundedBy xmlns='http://www.opengis.net/gml'>"
                + "                    <Envelope srsName='urn:ogc:def:crs:EPSG::28992'>"
                + "                        <lowerCorner>-7000 288999</lowerCorner>"
                + "                        <upperCorner>300000 629001</upperCorner>"
                + "                    </Envelope>"
                + "                </boundedBy>"
                + "            </dcx-gml:spatial>"
                + "            <dcx-gml:spatial>"
                + "                <boundedBy xmlns='http://www.opengis.net/gml'>"
                + "                    <Envelope><!-- no srsName -->"
                + "                        <lowerCorner>-7000</lowerCorner>"
                + "                    </Envelope>"
                + "                </boundedBy>"
                + "            </dcx-gml:spatial>"
                + "    </ddm:dcmiMetadata>"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new PointsHaveAtLeastTwoValues(reader).validate(Path.of("bagdir"));
        assertThat(result.getException()).isNull();
        assertThat(result.getStatus()).isEqualTo(RuleResult.Status.ERROR);
        assertThat(result.getErrorMessages())
                .hasSameElementsAs(List.of(
                        "pos has less than two coordinates: 1.0",
                        "pos has non numeric coordinates: a 5",
                        "lowerCorner is outside RD bounds: -7001 289000", // x too small
                        "upperCorner is outside RD bounds: 300001 629000", // x too large
                        "lowerCorner is outside RD bounds: -7000 288999", // y too small
                        "upperCorner is outside RD bounds: 300000 629001", // y too large
                        "lowerCorner has less than two coordinates: -7000"));
    }

    @Test
    void pointIsValid() throws Exception {
        var xml = "<ddm:DDM\n"
                + "        xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "        xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\"\n"
                + "        xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "        xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "        xmlns:dcx-gml=\"http://easy.dans.knaw.nl/schemas/dcx/gml/\"\n"
                + "        xmlns:id-type=\"http://easy.dans.knaw.nl/schemas/vocab/identifier-type/\">\n"
                + "    <ddm:dcmiMetadata>\n"
                + "            <dct:spatial xsi:type='dcx-gml:SimpleGMLType'>"
                + "                <Point xmlns='http://www.opengis.net/gml'>"
                + "                    <pos>1 2</pos>"
                + "                </Point>"
                + "            </dct:spatial>"
                + "    </ddm:dcmiMetadata>"
                + "</ddm:DDM>";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new PointsHaveAtLeastTwoValues(reader).validate(Path.of("bagdir"));
        assertThat(result.getException()).isNull();
        assertThat(result.getStatus()).isEqualTo(Status.SUCCESS);
    }

    @Test
    void ddmMustHaveRightsHolderDeposit() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
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

        var result = new DdmMustHaveRightsHolderDeposit(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void ddmMustHaveRightsHolderDepositButItDoesntExist() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
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

        var result = new DdmMustHaveRightsHolderDeposit(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void ddmMustHaveRightsHolderMigration() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
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

        var result = new DdmMustHaveRightsHolderMigration(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void ddmMustHaveRightsHolderMigrationButItDoesntExist() throws Exception {
        var xml = "<ddm:DDM xmlns:ddm=\"http://schemas.dans.knaw.nl/dataset/ddm-v2/\"\n"
                + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
                + "         xmlns:dct=\"http://purl.org/dc/terms/\"\n"
                + "         xmlns:dcx-dai=\"http://easy.dans.knaw.nl/schemas/dcx/dai/\">\n"
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

        var result = new DdmMustHaveRightsHolderMigration(reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void containsNotJustMD5Manifest() throws Exception {
        var manifests = Set.of(
                new Manifest(StandardSupportedAlgorithms.SHA1),
                new Manifest(StandardSupportedAlgorithms.MD5)
        );

        Mockito.when(bagItMetadataReader.getBag(Mockito.any())).thenReturn(Optional.of(new Bag()));
        Mockito.when(bagItMetadataReader.getBagManifests(Mockito.any())).thenReturn(manifests);

        var result = new ContainsNotJustMD5Manifest(bagItMetadataReader).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void containsOnlyMD5Manifest() throws Exception {
        var manifests = Set.of(
                new Manifest(StandardSupportedAlgorithms.MD5)
        );

        Mockito.when(bagItMetadataReader.getBag(Mockito.any())).thenReturn(Optional.of(new Bag()));
        Mockito.when(bagItMetadataReader.getBagManifests(Mockito.any())).thenReturn(manifests);

        var result = new ContainsNotJustMD5Manifest(bagItMetadataReader).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void containsNoManifestsAtAll() throws Exception {
        var manifests = new HashSet<Manifest>();

        Mockito.when(bagItMetadataReader.getBag(Mockito.any())).thenReturn(Optional.of(new Bag()));
        Mockito.when(bagItMetadataReader.getBagManifests(Mockito.any())).thenReturn(manifests);

        var result = new ContainsNotJustMD5Manifest(bagItMetadataReader).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void organizationalIdentifierPrefixIsValid() throws Exception {
        Mockito.when(bagItMetadataReader.getSingleField(Mockito.any(), Mockito.any()))
                .thenReturn("USER1-organizational-identifier")
                .thenReturn("user001");

        var result = new OrganizationalIdentifierPrefixIsValid(bagItMetadataReader, organizationIdentifierPrefixValidator).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void organizationalIdentifierPrefixIsInvalid() throws Exception {
        Mockito.when(bagItMetadataReader.getSingleField(Mockito.any(), Mockito.any()))
                .thenReturn("WRONG-organizational-identifier")
                .thenReturn("user001");

        var result = new OrganizationalIdentifierPrefixIsValid(bagItMetadataReader, organizationIdentifierPrefixValidator).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void organizationalIdentifierPrefixIsMissing() throws Exception {
        Mockito.when(bagItMetadataReader.getSingleField(Mockito.any(), Mockito.any()))
                .thenReturn("WRONG-organizational-identifier")
                .thenReturn(null);

        var result = new OrganizationalIdentifierPrefixIsValid(bagItMetadataReader, organizationIdentifierPrefixValidator).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void optionalFileIsUtf8Decodable() throws Exception {
        Mockito.when(fileService.exists(Mockito.any())).thenReturn(true);
        Mockito.when(fileService.readFileContents(Mockito.any(), Mockito.any())).thenReturn(CharBuffer.allocate(1));

        var result = new OptionalFileIsUtf8Decodable(Path.of("somefile.txt"), fileService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void optionalFileIsUtf8DecodableAndDoesNotExist() throws Exception {
        Mockito.when(fileService.exists(Mockito.any())).thenReturn(false);
        Mockito.when(fileService.readFileContents(Mockito.any(), Mockito.any())).thenReturn(CharBuffer.allocate(1));

        var result = new OptionalFileIsUtf8Decodable(Path.of("somefile.txt"), fileService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SKIP_DEPENDENCIES, result.getStatus());
    }

    @Test
    void optionalFileIsUtf8DecodableButThrowsException() throws Exception {
        Mockito.when(fileService.exists(Mockito.any())).thenReturn(true);
        Mockito.when(fileService.readFileContents(Mockito.any(), Mockito.any()))
                .thenThrow(new CharacterCodingException());

        var result = new OptionalFileIsUtf8Decodable(Path.of("somefile.txt"), fileService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void isOriginalFilepathsFileComplete() throws Exception {
        Mockito.when(originalFilepathsService.exists(Mockito.any())).thenReturn(true);
        Mockito.when(filesXmlService.readFilepaths(Mockito.any()))
                .thenReturn(Stream.of(
                        Path.of("data/1.txt"),
                        Path.of("data/2.txt")
                ));

        Mockito.when(fileService.getAllFiles(Mockito.any()))
                .thenReturn(List.of(
                        Path.of("bagdir/data/a.txt"),
                        Path.of("bagdir/data/b.txt")
                ));

        Mockito.when(originalFilepathsService.getMapping(Mockito.any()))
                .thenReturn(List.of(
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/1.txt"), Path.of("data/a.txt")),
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/2.txt"), Path.of("data/b.txt"))
                ));

        var result = new IsOriginalFilepathsFileComplete(originalFilepathsService, fileService, filesXmlService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void isOriginalFilepathsFileCompleteWithWrongMapping() throws Exception {
        Mockito.when(originalFilepathsService.exists(Mockito.any())).thenReturn(true);
        Mockito.when(filesXmlService.readFilepaths(Mockito.any()))
                .thenReturn(Stream.of(
                        Path.of("data/1.txt"),
                        Path.of("data/2.txt")
                ));

        Mockito.when(fileService.getAllFiles(Mockito.any()))
                .thenReturn(List.of(
                        Path.of("bagdir/data/a.txt"),
                        Path.of("bagdir/data/b.txt")
                ));

        Mockito.when(originalFilepathsService.getMapping(Mockito.any()))
                .thenReturn(List.of(
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/1.txt"), Path.of("data/a.txt")),
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/2.txt"), Path.of("data/c.txt")) // this one is wrong
                ));

        var result = new IsOriginalFilepathsFileComplete(originalFilepathsService, fileService, filesXmlService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void isOriginalFilepathsFileCompleteWithMissingMapping() throws Exception {
        Mockito.when(originalFilepathsService.exists(Mockito.any())).thenReturn(true);
        Mockito.when(filesXmlService.readFilepaths(Mockito.any()))
                .thenReturn(Stream.of(
                        Path.of("data/1.txt"),
                        Path.of("data/2.txt")
                ));

        Mockito.when(fileService.getAllFiles(Mockito.any()))
                .thenReturn(List.of(
                        Path.of("bagdir/data/a.txt"),
                        Path.of("bagdir/data/b.txt")
                ));

        Mockito.when(originalFilepathsService.getMapping(Mockito.any()))
                .thenReturn(List.of(
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/2.txt"), Path.of("data/b.txt")) // this one is wrong
                ));

        var result = new IsOriginalFilepathsFileComplete(originalFilepathsService, fileService, filesXmlService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void isOriginalFilepathsFileCompleteWithMissingFiles() throws Exception {
        Mockito.when(originalFilepathsService.exists(Mockito.any())).thenReturn(true);
        Mockito.when(filesXmlService.readFilepaths(Mockito.any()))
                .thenReturn(Stream.of(
                        Path.of("data/1.txt")
                ));

        Mockito.when(fileService.getAllFiles(Mockito.any()))
                .thenReturn(List.of(
                        Path.of("bagdir/data/a.txt")
                ));

        Mockito.when(originalFilepathsService.getMapping(Mockito.any()))
                .thenReturn(List.of(
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/1.txt"), Path.of("data/a.txt")),
                        new OriginalFilepathsService.OriginalFilePathItem(Path.of("data/2.txt"), Path.of("data/b.txt"))
                ));

        var result = new IsOriginalFilepathsFileComplete(originalFilepathsService, fileService, filesXmlService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void isOriginalFilepathsFileCompleteSkipped() throws Exception {
        Mockito.when(originalFilepathsService.exists(Mockito.any())).thenReturn(false);
        var result = new IsOriginalFilepathsFileComplete(originalFilepathsService, fileService, filesXmlService).validate(Path.of("bagdir"));

        assertEquals(RuleResult.Status.SKIP_DEPENDENCIES, result.getStatus());
    }
}
