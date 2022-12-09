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
package nl.knaw.dans.validatedansbag.resources;

import io.dropwizard.jersey.errors.ErrorMessage;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import nl.knaw.dans.validatedansbag.api.ValidateCommandDto;
import nl.knaw.dans.validatedansbag.api.ValidateCommandDto.LevelEnum;
import nl.knaw.dans.validatedansbag.api.ValidateCommandDto.PackageTypeEnum;
import nl.knaw.dans.validatedansbag.api.ValidateOkDto;
import nl.knaw.dans.validatedansbag.api.ValidateOkDto.InformationPackageTypeEnum;
import nl.knaw.dans.validatedansbag.api.ValidateOkRuleViolationsDto;
import nl.knaw.dans.validatedansbag.core.BagNotFoundException;
import nl.knaw.dans.validatedansbag.core.engine.DepositType;
import nl.knaw.dans.validatedansbag.core.engine.ValidationLevel;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineService;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.xml.sax.Locator;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.LocatorImpl;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipError;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(DropwizardExtensionsSupport.class)
class ValidateResourceTest {
    private final RuleEngineService ruleEngineService = Mockito.mock(RuleEngineService.class);
    private final FileService fileService = Mockito.mock(FileService.class);
    public final ResourceExtension EXT = ResourceExtension.builder()
        .addProvider(MultiPartFeature.class)
        .addResource(new ValidateResource(ruleEngineService, fileService))
        .build();

    @BeforeEach
    void setup() {
        Mockito.reset(fileService);
        Mockito.reset(ruleEngineService);
    }

    @Test
    void validateFormData() {
        var data = new ValidateCommandDto();
        data.setBagLocation("it/is/here");
        data.setPackageType(PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
            .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(Entity.entity(multipart, multipart.getMediaType()), Response.class);

        Mockito.verifyNoInteractions(fileService);//.extractZipFile(Mockito.any(Path.class));
    }

    @Test
    void validateFormDataWithZipFile() throws Exception {
        var data = new ValidateCommandDto();
        data.setBagLocation(null);
        data.setPackageType(PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
            .field("command", data, MediaType.APPLICATION_JSON_TYPE)
            .field("zip", new ByteArrayInputStream(new byte[4]), MediaType.valueOf("application/zip"));

        Mockito.doReturn(Path.of("/tmp/bag-1"))
            .when(fileService).extractZipFile(Mockito.any(InputStream.class));

        Mockito.doReturn(Optional.of(Path.of("bagdir")))
            .when(fileService).getFirstDirectory(Mockito.any());

        EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(Entity.entity(multipart, multipart.getMediaType()), Response.class);

        Mockito.verify(fileService).extractZipFile(Mockito.any(InputStream.class));//.extractZipFile(Mockito.any(Path.class));
    }

    @Test
    void validateFormDataWithInternalServerError() throws Exception {
        var data = new ValidateCommandDto();
        data.setBagLocation(null);
        data.setPackageType(PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
            .field("command", data, MediaType.APPLICATION_JSON_TYPE)
            .field("zip", new ByteArrayInputStream(new byte[4]), MediaType.valueOf("application/zip"));

        Mockito.doReturn(Path.of("/tmp/bag-1"))
            .when(fileService).extractZipFile(Mockito.any(InputStream.class));

        Mockito.doReturn(Optional.of(Path.of("/tmp/bag-1/bag")))
            .when(fileService).getFirstDirectory(Mockito.any());

        Mockito.doThrow(new IOException("Error deleting directory"))
            .when(fileService).deleteDirectoryAndContents(Mockito.any());
        // caught in validateInputStream.finally, not covered by ValidateResourceIntegrationTest

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(Entity.entity(multipart, multipart.getMediaType()), Response.class)) {

            assertEquals("{\"code\":500,\"message\":\"HTTP 500 Internal Server Error\"}", response.readEntity(String.class));
            assertEquals("Internal Server Error",response.getStatusInfo().getReasonPhrase());
            assertEquals(500, response.getStatus());
        }

        Mockito.verify(fileService).extractZipFile(Mockito.any(InputStream.class));//.extractZipFile(Mockito.any(Path.class));
    }

    @Test
    void validateZipFile() throws Exception {
        var zip = Entity.entity(new ByteArrayInputStream(new byte[4]), MediaType.valueOf("application/zip"));

        Mockito.doReturn(Path.of("/tmp/bag-1"))
            .when(fileService).extractZipFile(Mockito.any(InputStream.class));

        Mockito.doReturn(Optional.of(Path.of("bagdir")))
            .when(fileService).getFirstDirectory(Mockito.any());

        EXT.target("/validate")
            .request()
            .post(zip, Response.class);

        Mockito.verify(fileService).extractZipFile(Mockito.any(InputStream.class));//.extractZipFile(Mockito.any(Path.class));
    }

    @Test
    void validateMultipartFileButTheFileDoesNotExist() throws Exception {
        var data = new ValidateCommandDto();
        data.setBagLocation("some/path");
        data.setPackageType(PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
            .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        Mockito.doThrow(BagNotFoundException.class)
            .when(ruleEngineService).validateBag(Mockito.any(), Mockito.any(), Mockito.any());

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(Entity.entity(multipart, multipart.getMediaType()), Response.class)
        ) {
            assertEquals(400, response.getStatus());
        }
    }

    @Test
    void validateMultipartFileWithMissingDependencyOnSchemaValidation1() throws Exception {
        var data = new ValidateCommandDto();
        data.setBagLocation("some/path");
        data.setPackageType(PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
            .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        Mockito
            .when(ruleEngineService.validateBag(Mockito.any(), Mockito.any(), Mockito.any()))
            .thenThrow(new SAXParseException("some syntax problem", new Locator() {

                @Override
                public String getPublicId() {
                    return null;
                }

                @Override
                public String getSystemId() {
                    return "/path/to/bag/metadata/dataset.xml";
                }

                @Override
                public int getLineNumber() {
                    return 0;
                }

                @Override
                public int getColumnNumber() {
                    return 0;
                }
            }));
        ;

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(Entity.entity(multipart, multipart.getMediaType()), Response.class)
        ) {
            assertEquals(200, response.getStatus());
            ValidateOkDto actual = response.readEntity(ValidateOkDto.class);
            assertFalse(actual.getIsCompliant());
            ValidateOkRuleViolationsDto expected = new ValidateOkRuleViolationsDto();
            expected.setRule("not known");
            expected.setViolation("dataset.xml - line: 0; column: 0 msg: some syntax problem");
            assertEquals(List.of(expected), actual.getRuleViolations());

        }
    }

    @Test
    void validateMultipartFileWithMissingDependencyOnSchemaValidation2() throws Exception {
        var data = new ValidateCommandDto();
        data.setBagLocation("some/path");
        data.setPackageType(PackageTypeEnum.DEPOSIT);

        var multipart = new FormDataMultiPart()
            .field("command", data, MediaType.APPLICATION_JSON_TYPE);

        Mockito
            .when(ruleEngineService.validateBag(Mockito.any(), Mockito.any(), Mockito.any()))
            .thenThrow(new SAXParseException("some syntax problem", new LocatorImpl()));
        ;

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(Entity.entity(multipart, multipart.getMediaType()), Response.class)
        ) {
            assertEquals(200, response.getStatus());
            ValidateOkDto actual = response.readEntity(ValidateOkDto.class);
            assertFalse(actual.getIsCompliant());
            ValidateOkRuleViolationsDto expected = new ValidateOkRuleViolationsDto();
            expected.setRule("not known");
            expected.setViolation("some syntax problem");
            assertEquals(List.of(expected), actual.getRuleViolations());

        }
    }

    @Test
    void validateZipButItIsNotAValidZip() throws Exception {
        var zip = Entity.entity(new ByteArrayInputStream(new byte[4]), MediaType.valueOf("application/zip"));

        Mockito.doThrow(ZipError.class)
            .when(fileService).extractZipFile(Mockito.any(InputStream.class));

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(zip, Response.class)
        ) {
            assertEquals(500, response.getStatus());
        }
    }

    @Test
    void validateBagNotFoundExceptionReturnsBadRequest() throws Exception {
        var zip = Entity.entity(new ByteArrayInputStream(new byte[4]), MediaType.valueOf("application/zip"));

        Mockito.when(fileService.getFirstDirectory(Mockito.any()))
            .thenReturn(Optional.empty());

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(zip, Response.class)
        ) {
            assertEquals(400, response.getStatus());
            assertTrue(response.readEntity(String.class).contains("Extracted zip does not contain a directory"));
        }
    }

    @Test
    void validateIOErrorThrowsInternalServerError() throws Exception {
        var zip = Entity.entity(new ByteArrayInputStream(new byte[4]), MediaType.valueOf("application/zip"));

        Mockito.when(fileService.getFirstDirectory(Mockito.any()))
            .thenReturn(Optional.of(Path.of("something")));

        Mockito.doThrow(new IOException("Error deleting directory"))
            .when(fileService).deleteDirectoryAndContents(Mockito.any());

        try (var response = EXT.target("/validate")
            .register(MultiPartFeature.class)
            .request()
            .post(zip, Response.class)
        ) {
            assertEquals(500, response.getStatus());
            assertEquals(new ErrorMessage("HTTP 500 Internal Server Error"), response.readEntity(ErrorMessage.class));
        }
    }

    @Test
    void toDepositType() {
        var service = new ValidateResource(ruleEngineService, fileService);
        assertEquals(DepositType.DEPOSIT, service.toDepositType(PackageTypeEnum.DEPOSIT));
        assertEquals(DepositType.MIGRATION, service.toDepositType(PackageTypeEnum.MIGRATION));
        assertEquals(DepositType.DEPOSIT, service.toDepositType(null)); // this is the default case
    }

    @Test
    void toValidationLevel() {
        var service = new ValidateResource(ruleEngineService, fileService);
        assertEquals(ValidationLevel.STAND_ALONE, service.toValidationLevel(LevelEnum.STAND_ALONE));
        assertEquals(ValidationLevel.WITH_DATA_STATION_CONTEXT, service.toValidationLevel(LevelEnum.WITH_DATA_STATION_CONTEXT));
        assertEquals(ValidationLevel.STAND_ALONE, service.toValidationLevel(null)); // this is the default case
    }

    @Test
    void toInfoPackageType() {
        var service = new ValidateResource(ruleEngineService, fileService);
        assertEquals(InformationPackageTypeEnum.DEPOSIT, service.toInfoPackageType(DepositType.DEPOSIT));
        assertEquals(InformationPackageTypeEnum.MIGRATION, service.toInfoPackageType(DepositType.MIGRATION));
        assertEquals(InformationPackageTypeEnum.DEPOSIT, service.toInfoPackageType(null)); // this is the default case
    }

    @Test
    void toLevel() {
        var service = new ValidateResource(ruleEngineService, fileService);
        assertEquals(ValidateOkDto.LevelEnum.STAND_ALONE, service.toLevel(ValidationLevel.STAND_ALONE));
        assertEquals(ValidateOkDto.LevelEnum.WITH_DATA_STATION_CONTEXT, service.toLevel(ValidationLevel.WITH_DATA_STATION_CONTEXT));
        assertEquals(ValidateOkDto.LevelEnum.STAND_ALONE, service.toLevel(null)); // this is the default case
    }
}