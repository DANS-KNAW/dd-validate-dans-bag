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

import nl.knaw.dans.lib.dataverse.model.DataMessage;
import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import nl.knaw.dans.validatedansbag.resources.util.MockedDataverseResponse;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatasetXmlEmbargoPeriodWithinLimitsTest extends RuleTestFixture {

    @Test
    void should_return_ERROR_when_date_available_too_far_in_the_future() throws Exception {
        int maximumEmbargoPeriodInMonths = 4;
        DateTime dateAvailable = new DateTime(DateTime.now().plusMonths(maximumEmbargoPeriodInMonths).plusDays(1));
        final String xml = String.format("""
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:profile>
                    <ddm:available>%s</ddm:available>
                </ddm:profile>
            </ddm:DDM>""", DateTimeFormat.forPattern("yyyy-MM-dd").print(dateAvailable));

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var embargoResultJson = String.format("""
            {
              "status": "OK",
              "data": {
                "message": "%d"
              }
            }""", maximumEmbargoPeriodInMonths);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var result = new DatasetXmlEmbargoPeriodWithinLimits(dataverseService, reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

    @Test
    void should_return_SUCCESS_when_date_available_on_last_allowed_day() throws Exception {
        int maximumEmbargoPeriodInMonths = 4;
        DateTime dateAvailable = new DateTime(DateTime.now().plusMonths(maximumEmbargoPeriodInMonths));
        final String xml = String.format("""
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:profile>
                    <ddm:available>%s</ddm:available>
                </ddm:profile>
            </ddm:DDM>""", DateTimeFormat.forPattern("yyyy-MM-dd").print(dateAvailable));

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var embargoResultJson = String.format("""
            {
              "status": "OK",
              "data": {
                "message": "%d"
              }
            }""", maximumEmbargoPeriodInMonths);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var result = new DatasetXmlEmbargoPeriodWithinLimits(dataverseService, reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void should_return_SUCCESS_when_date_available_has_no_day_part() throws Exception {
        int maximumEmbargoPeriodInMonths = 4;
        String dateAvailable = "2013-12"; // In the past, so should be OK
        final String xml = String.format("""
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:profile>
                    <ddm:available>%s</ddm:available>
                </ddm:profile>
            </ddm:DDM>""", dateAvailable);

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var embargoResultJson = String.format("""
            {
              "status": "OK",
              "data": {
                "message": "%d"
              }
            }""", maximumEmbargoPeriodInMonths);
        var maxEmbargoDurationResult = new MockedDataverseResponse<DataMessage>(embargoResultJson, DataMessage.class);
        Mockito.when(dataverseService.getMaxEmbargoDurationInMonths())
                .thenReturn(maxEmbargoDurationResult);

        var result = new DatasetXmlEmbargoPeriodWithinLimits(dataverseService, reader).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }
}
