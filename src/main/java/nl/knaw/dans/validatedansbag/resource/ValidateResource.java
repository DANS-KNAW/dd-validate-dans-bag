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
package nl.knaw.dans.validatedansbag.resource;

import nl.knaw.dans.openapi.api.ValidateCommandDto;
import nl.knaw.dans.openapi.api.ValidateOkDto;
import nl.knaw.dans.openapi.api.ValidateOkDto.InformationPackageTypeEnum;
import nl.knaw.dans.openapi.api.ValidateOkDto.LevelEnum;
import nl.knaw.dans.openapi.api.ValidateOkRuleViolationsDto;
import nl.knaw.dans.validatedansbag.core.BagNotFoundException;
import nl.knaw.dans.validatedansbag.core.engine.DepositType;
import nl.knaw.dans.validatedansbag.core.engine.RuleValidationResult;
import nl.knaw.dans.validatedansbag.core.engine.ValidationLevel;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineService;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

@Path("/validate")
public class ValidateResource {

    private static final Logger log = LoggerFactory.getLogger(ValidateResource.class);

    private final RuleEngineService ruleEngineService;

    private final FileService fileService;

    public ValidateResource(RuleEngineService ruleEngineService, FileService fileService) {
        this.ruleEngineService = ruleEngineService;
        this.fileService = fileService;
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public ValidateOkDto validateFormData(
        @Valid @NotNull @FormDataParam(value = "command") ValidateCommandDto command,
        @FormDataParam(value = "zip") InputStream zipInputStream
    ) {
        var location = command.getBagLocation();
        var depositType = toDepositType(command.getPackageType());
        var validationLevel = toValidationLevel(command.getLevel());

        log.info("Received request to validate bag: {}", command);

        try {
            ValidateOkDto validateResult;

            if (location == null) {
                validateResult = validateInputStream(zipInputStream, depositType, validationLevel);
            }
            else {
                var locationPath = java.nio.file.Path.of(location);
                validateResult = validatePath(locationPath, depositType, validationLevel);
            }

            // this information is lost during the validation, so set it again here
            validateResult.setBagLocation(location);

            return validateResult;
        }
        catch (BagNotFoundException e) {
            log.error("Bag not found", e);
            throw new BadRequestException("Request could not be processed: " + e.getMessage(), e);
        }
        catch (Exception e) {
            log.error("Internal server error", e);
            throw new InternalServerErrorException();
        }
    }

    @POST
    @Consumes({ "application/zip" })
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public ValidateOkDto validateZip(InputStream inputStream, @QueryParam("level") ValidationLevel level) {
        if (level == null) {
            level = ValidationLevel.STAND_ALONE;
        }

        try {
            log.info("Received request to validate zip file");
            return validateInputStream(inputStream, DepositType.DEPOSIT, level);
        }
        catch (BagNotFoundException e) {
            log.error("Bag not found", e);
            throw new BadRequestException("Request could not be processed: " + e.getMessage(), e);
        }
        catch (Exception e) {
            log.error("Internal server error", e);
            throw new InternalServerErrorException();
        }
    }

    ValidateOkDto validateInputStream(InputStream inputStream, DepositType depositType, ValidationLevel validationLevel) throws Exception {
        var tempPath = fileService.extractZipFile(inputStream);

        try {
            var bagDir = fileService.getFirstDirectory(tempPath)
                .orElseThrow(() -> new BagNotFoundException("Extracted zip does not contain a directory"));

            return validatePath(bagDir, depositType, validationLevel);
        }
        finally {
            fileService.deleteDirectoryAndContents(tempPath);
        }
    }

    ValidateOkDto validatePath(java.nio.file.Path bagDir, DepositType depositType, ValidationLevel validationLevel) throws Exception {

        List<RuleValidationResult> results = null;
        String violationMessage = null;
        try {
            results = ruleEngineService.validateBag(bagDir, depositType, validationLevel);
        }
        catch (SAXParseException e) {
            violationMessage = String.format("%s - line: %d; column: %d msg: %s", new File(e.getSystemId()).getName(), e.getLineNumber(), e.getColumnNumber(), e.getMessage());
            log.error("An xmlFileConformsToSchema is missing or a dependency on such rule", e);
        }

        var isValid = results != null && results.stream().noneMatch(r -> r.getStatus().equals(RuleValidationResult.RuleValidationResultStatus.FAILURE));

        var result = new ValidateOkDto();
        result.setBagLocation(null);
        result.setIsCompliant(isValid);
        result.setName(bagDir.getFileName().toString());
        result.setProfileVersion("1.0.0");
        result.setInformationPackageType(toInfoPackageType(depositType));
        result.setLevel(toLevel(validationLevel));
        if (results == null) {
            ValidateOkRuleViolationsDto violation = new ValidateOkRuleViolationsDto();
            violation.setViolation(violationMessage);
            violation.setRule("not known");
            result.setRuleViolations(List.of(violation));
        }
        else
            result.setRuleViolations(results.stream()
            .filter(r -> r.getStatus().equals(RuleValidationResult.RuleValidationResultStatus.FAILURE))
            .map(rule -> {
                var ret = new ValidateOkRuleViolationsDto();
                ret.setRule(rule.getNumber());
                ret.setViolation(rule.getErrorMessage());
                return ret;
            })
            .collect(Collectors.toList()));

        log.debug("Validation result: {}", result);

        return result;
    }

    DepositType toDepositType(ValidateCommandDto.PackageTypeEnum value) {
        if (ValidateCommandDto.PackageTypeEnum.MIGRATION.equals(value)) {
            return DepositType.MIGRATION;
        }
        return DepositType.DEPOSIT;
    }

    ValidationLevel toValidationLevel(ValidateCommandDto.LevelEnum value) {
        if (ValidateCommandDto.LevelEnum.WITH_DATA_STATION_CONTEXT.equals(value)) {
            return ValidationLevel.WITH_DATA_STATION_CONTEXT;
        }
        return ValidationLevel.STAND_ALONE;
    }

    InformationPackageTypeEnum toInfoPackageType(DepositType value) {
        if (DepositType.MIGRATION.equals(value)) {
            return InformationPackageTypeEnum.MIGRATION;
        }
        return InformationPackageTypeEnum.DEPOSIT;
    }

    LevelEnum toLevel(ValidationLevel value) {
        if (ValidationLevel.WITH_DATA_STATION_CONTEXT.equals(value)) {
            return LevelEnum.WITH_DATA_STATION_CONTEXT;
        }
        return LevelEnum.STAND_ALONE;
    }
}
