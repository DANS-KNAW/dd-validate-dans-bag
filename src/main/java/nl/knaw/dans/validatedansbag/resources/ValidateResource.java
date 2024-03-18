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

import nl.knaw.dans.validatedansbag.api.ValidateCommandDto;
import nl.knaw.dans.validatedansbag.api.ValidateOkDto;
import nl.knaw.dans.validatedansbag.api.ValidateOkRuleViolationsInnerDto;
import nl.knaw.dans.validatedansbag.core.BagNotFoundException;
import nl.knaw.dans.validatedansbag.core.engine.DepositType;
import nl.knaw.dans.validatedansbag.core.engine.RuleValidationResult;
import nl.knaw.dans.validatedansbag.core.service.FileService;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineService;
import nl.knaw.dans.validatedansbag.core.validator.SecurePathValidator;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

@Path("/validate")
public class ValidateResource {

    private static final Logger log = LoggerFactory.getLogger(ValidateResource.class);

    private final RuleEngineService ruleEngineService;

    private final FileService fileService;
    private final SecurePathValidator pathSecurityValidator;

    public ValidateResource(RuleEngineService ruleEngineService, FileService fileService, SecurePathValidator pathSecurityValidator) {
        this.ruleEngineService = ruleEngineService;
        this.fileService = fileService;
        this.pathSecurityValidator = pathSecurityValidator;
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

        log.info("Received request to validate bag: {}", command);

        try {
            ValidateOkDto validateResult;

            if (location == null) {
                validateResult = validateInputStream(zipInputStream, depositType);
            }
            else {
                var locationPath = java.nio.file.Path.of(location);
                if (!this.pathSecurityValidator.IsPathSecure(locationPath))
                    throw new IllegalArgumentException(String.format("InsecurePath: %s", locationPath));
                validateResult = validatePath(locationPath, depositType);
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
            throw new InternalServerErrorException("Internal server error", e);
        }
    }

    @POST
    @Consumes({ "application/zip" })
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public ValidateOkDto validateZip(InputStream inputStream) {
        try {
            log.info("Received request to validate zip file");
            return validateInputStream(inputStream, DepositType.DEPOSIT);
        }
        catch (BagNotFoundException e) {
            log.error("Bag not found", e);
            throw new BadRequestException("Request could not be processed: " + e.getMessage(), e);
        }
        catch (Exception e) {
            log.error("Internal server error", e);
            throw new InternalServerErrorException("Internal server error", e);
        }
    }

    ValidateOkDto validateInputStream(InputStream inputStream, DepositType depositType) throws Exception {
        var tempPath = fileService.extractZipFile(inputStream);

        try {
            var bagDir = fileService.getFirstDirectory(tempPath)
                .orElseThrow(() -> new BagNotFoundException("Extracted zip does not contain a directory"));

            return validatePath(bagDir, depositType);
        }
        finally {
            try {
                fileService.deleteDirectoryAndContents(tempPath);
            }
            catch (IOException e) {
                log.error("Error cleaning up temporary directory");
            }
        }

    }

    ValidateOkDto validatePath(java.nio.file.Path bagDir, DepositType depositType) throws Exception {
        var results = ruleEngineService.validateBag(bagDir, depositType);
        var isValid = results.stream().noneMatch(r -> r.getStatus().equals(RuleValidationResult.RuleValidationResultStatus.FAILURE));

        var result = new ValidateOkDto();
        result.setBagLocation(null);
        result.setIsCompliant(isValid);
        result.setName(bagDir.getFileName().toString());
        result.setProfileVersion("1.0.0");
        result.setInformationPackageType(toInfoPackageType(depositType));
        result.setRuleViolations(results.stream()
            .filter(r -> r.getStatus().equals(RuleValidationResult.RuleValidationResultStatus.FAILURE))
            .map(rule -> {
                var ret = new ValidateOkRuleViolationsInnerDto();
                ret.setRule(rule.getNumber());

                var message = new StringBuilder();

                if (rule.getErrorMessage() != null) {
                    message.append(rule.getErrorMessage());
                }

                ret.setViolation(message.toString());
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

    ValidateOkDto.InformationPackageTypeEnum toInfoPackageType(DepositType value) {
        if (DepositType.MIGRATION.equals(value)) {
            return ValidateOkDto.InformationPackageTypeEnum.MIGRATION;
        }
        return ValidateOkDto.InformationPackageTypeEnum.DEPOSIT;
    }
}
