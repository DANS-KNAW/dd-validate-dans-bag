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

import lombok.AllArgsConstructor;
import nl.knaw.dans.validatedansbag.api.ValidateCommandDto;
import nl.knaw.dans.validatedansbag.core.BagNotFoundException;
import nl.knaw.dans.validatedansbag.core.engine.DepositType;
import nl.knaw.dans.validatedansbag.core.service.RuleEngineService;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.nio.file.Path;

@AllArgsConstructor
public class ValidateLocalDirApiResource implements ValidateLocalDirApi {
    private final RuleEngineService ruleEngineService;

    @Override
    public Response validateLocalDirPost(ValidateCommandDto validateCommandDto) {
        try {
            var result = ruleEngineService.validateBag(Path.of(validateCommandDto.getBagLocation()),
                DepositType.valueOf(validateCommandDto.getPackageType().toString()),
                validateCommandDto.getBagLocation());
            return Response.ok(result).build();
        }
        catch (BagNotFoundException e) {
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
