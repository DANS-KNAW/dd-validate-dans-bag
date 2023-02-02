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
package nl.knaw.dans.validatedansbag.resources.util;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;
import nl.knaw.dans.validatedansbag.core.auth.SwordUser;

import java.util.Optional;

public class MockAuthorization implements Authenticator<BasicCredentials, SwordUser> {

    @Override
    public Optional<SwordUser> authenticate(BasicCredentials basicCredentials) throws AuthenticationException {
        if ("user001".equals(basicCredentials.getUsername()) && "user001".equals(basicCredentials.getPassword())) {
            return Optional.of(new SwordUser("user001"));
        }
        return Optional.empty();
    }
}
