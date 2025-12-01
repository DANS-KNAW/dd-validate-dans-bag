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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.ruleengine.NumberedRule;
import nl.knaw.dans.lib.util.ruleengine.RuleEngine;
import nl.knaw.dans.lib.util.ruleengine.RuleEngineConfigurationException;
import nl.knaw.dans.lib.util.ruleengine.RuleValidationResult;
import nl.knaw.dans.validatedansbag.api.ValidateOkDto;
import nl.knaw.dans.validatedansbag.api.ValidateOkRuleViolationsInnerDto;
import nl.knaw.dans.validatedansbag.core.BagNotFoundException;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class RuleEngineServiceImpl implements RuleEngineService {

    private final RuleEngine ruleEngine;
    private final FileService fileService;
    private final List<NumberedRule> ruleSet;

    public RuleEngineServiceImpl(RuleEngine ruleEngine,
        FileService fileService,
        List<NumberedRule> ruleSet) {
        this.ruleEngine = ruleEngine;
        this.fileService = fileService;
        this.ruleSet = ruleSet;
        this.validateRuleConfiguration();
    }

    @Override
    public ValidateOkDto validateBag(Path path, String bagLocation) throws Exception {
        log.info("Validating bag on path '{}'", path);

        if (!fileService.isReadable(path)) {
            log.warn("Path {} could not not be found or is not readable", path);
            throw new BagNotFoundException(String.format("Bag on path '%s' could not be found or read", path));
        }

        var results = ruleEngine.validateBag(path, this.ruleSet);
        var isValid = results.stream().noneMatch(r -> r.getStatus().equals(RuleValidationResult.RuleValidationResultStatus.FAILURE));

        var result = new ValidateOkDto();
        result.setBagLocation(bagLocation);
        result.setIsCompliant(isValid);
        result.setName(path.getFileName().toString());
        result.setProfileVersion("1.2.0");
        result.setInformationPackageType(ValidateOkDto.InformationPackageTypeEnum.DEPOSIT);
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

    public void validateRuleConfiguration() {
        try {
            this.ruleEngine.validateRuleSet(this.ruleSet);
        }
        catch (RuleEngineConfigurationException e) {
            throw new RuntimeException("Rule configuration is not valid", e);
        }
    }
}
