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

import nl.knaw.dans.validatedansbag.core.engine.RuleViolationDetailsException;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.OriginalFilepathsService;
import nl.knaw.dans.validatedansbag.core.service.XmlReader;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilesXmlRulesImplTest {

    final FileService fileService = Mockito.mock(FileService.class);
    final XmlReader xmlReader = Mockito.spy(new XmlReaderImpl());
    final OriginalFilepathsService originalFilepathsService = Mockito.mock(OriginalFilepathsService.class);

    @AfterEach
    void afterEach() {
        Mockito.reset(fileService);
        Mockito.reset(xmlReader);
        Mockito.reset(originalFilepathsService);
    }

    private Document parseXmlString(String str) throws ParserConfigurationException, IOException, SAXException {
        return new XmlReaderImpl().readXmlString(str);
    }

    @Test
    void filesXmlFilePathAttributesContainLocalBagPathAndNonPayloadFilesAreNotDescribed() throws Exception, RuleViolationDetailsException {
        var checker = Mockito.spy(new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService));
        Mockito.doNothing().when(checker).filesXmlFileElementsAllHaveFilepathAttribute(Mockito.any());
        Mockito.doNothing().when(checker).filesXmlDescribesOnlyPayloadFiles(Mockito.any());

        assertDoesNotThrow(() -> checker.filesXmlFilePathAttributesContainLocalBagPathAndNonPayloadFilesAreNotDescribed().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlFilePathAttributesContainLocalBagPathAndNonPayloadFilesAreNotDescribedThrowsDoubleError() throws Exception, RuleViolationDetailsException {
        var checker = Mockito.spy(new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService));
        Mockito.doThrow(new RuleViolationDetailsException("msg1")).when(checker).filesXmlFileElementsAllHaveFilepathAttribute(Mockito.any());
        Mockito.doThrow(new RuleViolationDetailsException("msg1")).when(checker).filesXmlDescribesOnlyPayloadFiles(Mockito.any());

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlFilePathAttributesContainLocalBagPathAndNonPayloadFilesAreNotDescribed().validate(Path.of("bagdir")));

        assertFalse(e.isMultiException());
    }

    @Test
    void filesXmlNoDuplicateFilesAndEveryPayloadFileIsDescribed() throws Exception, RuleViolationDetailsException {
        var checker = Mockito.spy(new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService));
        Mockito.doNothing().when(checker).filesXmlNoDuplicates(Mockito.any());
        Mockito.doNothing().when(checker).filesXmlDescribesAllPayloadFiles(Mockito.any());

        assertDoesNotThrow(() -> checker.filesXmlNoDuplicateFilesAndEveryPayloadFileIsDescribed().validate(Path.of("bagdir")));
    }

    @Test
    void filesXmlNoDuplicateFilesAndEveryPayloadFileIsDescribedThrowsDoubleError() throws Exception, RuleViolationDetailsException {
        var checker = Mockito.spy(new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService));
        Mockito.doThrow(new RuleViolationDetailsException("msg1")).when(checker).filesXmlNoDuplicates(Mockito.any());
        Mockito.doThrow(new RuleViolationDetailsException("msg2")).when(checker).filesXmlDescribesAllPayloadFiles(Mockito.any());

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlNoDuplicateFilesAndEveryPayloadFileIsDescribed().validate(Path.of("bagdir")));

        assertEquals(2, e.getExceptions().size());
    }

    @Test
    void filesXmlDescribesOnlyPayloadFiles() throws Exception {
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
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        // even though image02 is not defined in the files.xml, this partial rule does not check for that
        var files = List.of(
            Path.of("bagdir/data/random images/image01.png"),
            Path.of("bagdir/data/random images/image02.png")
        );

        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);

        assertDoesNotThrow(() ->
            checker.filesXmlDescribesOnlyPayloadFiles(Path.of("bagdir")));
    }

    @Test
    void filesXmlDescribesMoreThanOnlyPayloadFiles() throws Exception {
        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "        <dcterms:description>This description will be archived, but not displayed anywhere in the Web-UI</dcterms:description>\n"
            + "        <dcterms:format>image/png</dcterms:format>\n"
            + "        <dcterms:created>2016-11-11</dcterms:created>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var files = List.of(
            Path.of("bagdir/data/random images/image01.png")
        );

        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);

        var e = assertThrows(RuleViolationDetailsException.class, () ->
            checker.filesXmlDescribesOnlyPayloadFiles(Path.of("bagdir")));

        assertTrue(e.getMessage().contains("image02.png"));
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
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);

        assertDoesNotThrow(() -> checker.filesXmlFileElementsAllHaveFilepathAttribute(Path.of("bagdir")));
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
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);

        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlFileElementsAllHaveFilepathAttribute(Path.of("bagdir")));

        assertTrue(e.getLocalizedMessage().startsWith("2 "));
    }

    @Test
    void filesXmlNoDuplicates() throws Exception {

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
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);

        assertDoesNotThrow(() -> checker.filesXmlNoDuplicates(Path.of("bagdir")));
    }

    @Test
    void filesXmlNoDuplicatesButThereAreDuplicates() throws Exception {

        var xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<files xmlns=\"http://easy.dans.knaw.nl/schemas/bag/metadata/files/\" xmlns:dcterms=\"http://purl.org/dc/terms/\">\n"
            + "    <file filepath=\"data/random images/image01.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "    <file filepath=\"data/random images/image02.png\">\n"
            + "        <dcterms:title>The first image</dcterms:title>\n"
            + "    </file>\n"
            + "</files>\n"
            + "\n";

        var document = parseXmlString(xml);
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);

        var e = assertThrows(RuleViolationDetailsException.class, () -> checker.filesXmlNoDuplicates(Path.of("bagdir")));
        assertTrue(e.getMessage().contains("image02.png"));
    }

    @Test
    void filesXmlDescribesAllPayloadFiles() throws Exception {

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
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var files = List.of(
            Path.of("bagdir/data/random images/image01.png"),
            Path.of("bagdir/data/random images/image02.png"),
            Path.of("bagdir/data/random images/image03.png")
        );

        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);
        assertDoesNotThrow(() -> checker.filesXmlDescribesAllPayloadFiles(Path.of("bagdir")));
    }

    @Test
    void filesXmlDescribesAllPayloadFilesButMissesOne() throws Exception {

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
        Mockito.doReturn(document).when(xmlReader).readXmlFile(Mockito.any());

        var files = List.of(
            Path.of("bagdir/data/random images/image01.png"),
            Path.of("bagdir/data/random images/image02.png"),
            Path.of("bagdir/data/random images/image03.png"),
            Path.of("bagdir/data/random images/image04.png")
        );

        Mockito.doReturn(files).when(fileService).getAllFiles(Mockito.any());

        var checker = new FilesXmlRulesImpl(xmlReader, fileService, originalFilepathsService);
        var e = assertThrows(RuleViolationDetailsException.class,
            () -> checker.filesXmlDescribesAllPayloadFiles(Path.of("bagdir")));

        assertTrue(e.getMessage().contains("image04.png"));
    }
}