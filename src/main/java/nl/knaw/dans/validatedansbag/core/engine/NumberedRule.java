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
package nl.knaw.dans.validatedansbag.core.engine;

import nl.knaw.dans.validatedansbag.core.rules.BagValidatorRule;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class NumberedRule {
    private final String number;
    private final BagValidatorRule rule;
    private final List<String> dependencies;
    private DepositType depositType;
    private ValidationContext validationContext;

    public NumberedRule(String number, BagValidatorRule rule, String... dependencies) {
        this.number = Objects.requireNonNull(number, "Number must not be null");
        this.rule = Objects.requireNonNull(rule, "Rule must not be null");
        this.dependencies = Collections.unmodifiableList(List.of(dependencies));
        this.depositType = depositType;
        this.validationContext = Objects.requireNonNullElse(validationContext, ValidationContext.ALWAYS);
    }

    private NumberedRule(String number, BagValidatorRule rule, List<String> dependencies, DepositType depositType, ValidationContext validationContext) {
        this.number = Objects.requireNonNull(number, "Number must not be null");
        this.rule = Objects.requireNonNull(rule, "Rule must not be null");
        this.dependencies = Collections.unmodifiableList(dependencies);
        this.depositType = depositType;
        this.validationContext = Objects.requireNonNullElse(validationContext, ValidationContext.ALWAYS);
    }

    public ValidationContext getValidationContext() {
        return validationContext;
    }

    private void setValidationContext(ValidationContext validationContext) {
        this.validationContext = validationContext;
    }

    public DepositType getDepositType() {
        return depositType;
    }

    private void setDepositType(DepositType depositType) {
        this.depositType = depositType;
    }

    public String getNumber() {
        return number;
    }

    public BagValidatorRule getRule() {
        return rule;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return "NumberedRule{" +
            "number='" + number + '\'' +
            ", rule=" + rule +
            ", dependencies=" + dependencies +
            ", depositType=" + depositType +
            ", validationContext=" + validationContext +
            '}';
    }

    public static class Builder {
        private final NumberedRule numberedRule;

        public Builder(String number, BagValidatorRule rule, String... dependencies) {
            this.numberedRule = new NumberedRule(number, rule, List.of(dependencies), null, ValidationContext.ALWAYS);
        }

        public Builder withDepositType(DepositType depositType) {
            this.numberedRule.setDepositType(depositType);
            return this;
        }

        public Builder withValidationContext(ValidationContext validationContext) {
            this.numberedRule.setValidationContext(validationContext);
            return this;
        }

        public NumberedRule build() {
            return numberedRule;
        }
    }

}
