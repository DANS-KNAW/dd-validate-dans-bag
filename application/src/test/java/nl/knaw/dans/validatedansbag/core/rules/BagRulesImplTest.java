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


    private Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(str);
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
