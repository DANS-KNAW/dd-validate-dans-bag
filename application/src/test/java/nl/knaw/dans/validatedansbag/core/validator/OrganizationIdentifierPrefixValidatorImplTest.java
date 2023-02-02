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
package nl.knaw.dans.validatedansbag.core.validator;

import nl.knaw.dans.validatedansbag.core.config.OtherIdPrefix;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrganizationIdentifierPrefixValidatorImplTest {

    final List<OtherIdPrefix> prefixes = List.of(
        new OtherIdPrefix("user001", "user1-"),
        new OtherIdPrefix("user002", "U2:")
    );

    @Test
    void hasValidPrefix_should_return_true_if_prefixes_match() {
        var validator = new OrganizationIdentifierPrefixValidatorImpl(prefixes);
        assertTrue(validator.hasValidPrefix("user001", "user1-123"));
        assertTrue(validator.hasValidPrefix("user002", "U2:123456"));
    }

    @Test
    void hasValidPrefix_should_return_false_if_prefixes_do_not_match() {
        var validator = new OrganizationIdentifierPrefixValidatorImpl(prefixes);
        assertFalse(validator.hasValidPrefix("user001", "random"));
        assertFalse(validator.hasValidPrefix("user002", "user1-test"));
    }

    @Test
    void hasValidPrefix_should_return_false_if_user_is_not_in_config_list() {
        var validator = new OrganizationIdentifierPrefixValidatorImpl(prefixes);
        assertFalse(validator.hasValidPrefix("user003", "U2:123"));
    }
}