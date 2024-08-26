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

import nl.knaw.dans.validatedansbag.core.engine.RuleResult;
import nl.knaw.dans.validatedansbag.core.service.XmlReaderImpl;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatasetXmlGmlPolygonPosListIsWellFormedTest extends RuleTestFixture {

    @Test
    void should_return_SUCCESS_when_list_is_wellformed() throws Exception {
        var xml = """
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:dcmiMetadata>
                      <dcx-gml:spatial>
                        <MultiSurface xmlns="http://www.opengis.net/gml">
                            <name>A random surface with multiple polygons</name>
                            <surfaceMember>
                                <Polygon>
                                    <description>A triangle between BP, De Horeca Academie en the railway station</description>
                                    <exterior>
                                        <LinearRing>
                                            <posList>52.079710 4.342778 52.079710 4.342778 52.07913 4.34332 52.079710 4.342778</posList>
                                        </LinearRing>
                                    </exterior>
                                </Polygon>
                            </surfaceMember>
                            <surfaceMember>
                                <Polygon>
                                    <description>A triangle between BP, De Horeca Academie en the railway station</description>
                                    <exterior>
                                        <LinearRing>
                                            <posList>52.079710 4.342778 52.079710 4.342778 52.07913 4.34332 52.079710 4.342778</posList>
                                        </LinearRing>
                                    </exterior>
                                </Polygon>
                        </surfaceMember>
                        </MultiSurface>
              </dcx-gml:spatial>\
                </ddm:dcmiMetadata>
            </ddm:DDM>""";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlGmlPolygonPosListIsWellFormed(reader, polygonListValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.SUCCESS, result.getStatus());
    }

    @Test
    void should_return_ERROR_when_list_is_malformed() throws Exception {
        var xml = """
            <ddm:DDM
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
                    xmlns:ddm="http://schemas.dans.knaw.nl/dataset/ddm-v2/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
                    xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/">
                <ddm:dcmiMetadata>
                      <dcx-gml:spatial>
                        <MultiSurface xmlns="http://www.opengis.net/gml">
                            <name>A random surface with multiple polygons</name>
                            <surfaceMember>
                                <Polygon>
                                    <description>A triangle between BP, De Horeca Academie en the railway station</description>
                                    <exterior>
                                        <LinearRing>
                                            <posList>52.079710 4.342778 52.079710 4.342778 52.079710 4.342778</posList>
                                        </LinearRing>
                                    </exterior>
                                </Polygon>
                            </surfaceMember>
                            <surfaceMember>
                                <Polygon>
                                    <description>A triangle between BP, De Horeca Academie en the railway station</description>
                                    <exterior>
                                        <LinearRing>
                                            <posList>52.079710 4.342778 52.079710 4.342778 52.07913 4.34332 52.079710 4.342778</posList>
                                        </LinearRing>
                                    </exterior>
                                </Polygon>
                            </surfaceMember>
                        </MultiSurface>
                </dcx-gml:spatial>
                </ddm:dcmiMetadata>
            </ddm:DDM>""";

        var document = parseXmlString(xml);
        var reader = Mockito.spy(new XmlReaderImpl());

        Mockito.doReturn(document).when(reader).readXmlFile(Mockito.any());

        var result = new DatasetXmlGmlPolygonPosListIsWellFormed(reader, polygonListValidator).validate(Path.of("bagdir"));
        assertEquals(RuleResult.Status.ERROR, result.getStatus());
    }

}
