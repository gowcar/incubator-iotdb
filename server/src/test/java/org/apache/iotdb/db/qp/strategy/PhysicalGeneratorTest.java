/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp.strategy;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PhysicalGeneratorTest {
  PhysicalGenerator generator;
  String[] pathStrings;
  Path[] paths;
  MManager manager;

  //after deduplicate.
  Path[] deduplicatedPaths;
  Map<String, Set<String>> deviceToMeasurements = new HashMap<>();
  Map<String, Integer> pathToIndex = new HashMap<>();
  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    generator = new PhysicalGenerator();
    pathStrings = new String[] {
        "root.sg.d2.s2",
        "root.sg.d2.s1",
        "root.sg.d2.s2",
        "root.sg.d1.s1",
        "root.sg.d1.s1",
        "root.sg.d1.s3",
        "root.sg.d1.s2"
    };
    paths = new Path[pathStrings.length];
    int i = 0;
    for (String path : pathStrings) {
      paths[i++] = new Path(path);
    }


    deviceToMeasurements.put("root.sg.d1", new HashSet<>());
    deviceToMeasurements.get("root.sg.d1").add("s1");
    deviceToMeasurements.get("root.sg.d1").add("s2");
    deviceToMeasurements.get("root.sg.d1").add("s3");
    deviceToMeasurements.put("root.sg.d2", new HashSet<>());
    deviceToMeasurements.get("root.sg.d2").add("s1");
    deviceToMeasurements.get("root.sg.d2").add("s2");

    manager = MManager.getInstance();
    manager.init();
    for (String path : pathStrings) {
      try {
        manager.createTimeseries(path, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY,
            Collections
                .emptyMap());
      } catch (Exception e) {
        //nothing
      }
    }
  }

  @After
  public void tearDown() throws IOException {
    //manager.clear();
    //Files.delete(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()).toPath());
  }


  @Test
  public void testRawDataQueryPlanDeduplicate() throws MetadataException {
    deduplicatedPaths = new Path[] {
        paths[3], paths[6], paths[5], paths[1], paths[0]
    };
    int i = 0;
    for (Path path : deduplicatedPaths) {
      pathToIndex.put(path.getFullPath(), i++);
    }

    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setAlignByTime(true);
    plan.setOperatorType(OperatorType.QUERY);
    plan.setPaths(Arrays.asList(paths));

    generator.deduplicate(plan);
    for (i = 0; i < deduplicatedPaths.length; i ++) {
      Assert.assertEquals(deduplicatedPaths[i].getFullPath(), plan.getDeduplicatedPaths().get(i).getFullPath());
    }
    Assert.assertEquals(deduplicatedPaths.length, plan.getDeduplicatedDataTypes().size());
    for (Map.Entry<String, Integer> entry : plan.getPathToIndex().entrySet()) {
      Assert.assertEquals(pathToIndex.get(entry.getKey()), entry.getValue());
    }
    Assert.assertEquals(deviceToMeasurements.get("root.sg.d1").size(), plan.getAllMeasurementsInDevice("root.sg.d1").size());
    Assert.assertEquals(deviceToMeasurements.get("root.sg.d2").size(), plan.getAllMeasurementsInDevice("root.sg.d2").size());
  }

  @Test
  public void testAggregationPlanDeduplicate() throws MetadataException {
    deduplicatedPaths = new Path[] {
        paths[3], paths[4], paths[6], paths[5], paths[1], paths[0]
    };
    String[] deduplicatedAggregations = new String[] {
       "sum", "count", "sum", "count", "avg", "avg"
    };
    int i = 0;
    for (Path path : deduplicatedPaths) {
      pathToIndex.put(String.format("%s(%s)", deduplicatedAggregations[i] , path.getFullPath()), i++);
    }
    AggregationPlan plan = new AggregationPlan();
    plan.setAlignByTime(true);
    plan.setOperatorType(OperatorType.AGGREGATION);
    plan.setPaths(Arrays.asList(paths));
    plan.setAggregations(Arrays.asList("avg", "avg", "avg", "sum", "count", "count", "sum"));
    generator.deduplicate(plan);
    for (i = 0; i < deduplicatedPaths.length; i ++) {
      Assert.assertEquals(deduplicatedPaths[i].getFullPath(), plan.getDeduplicatedPaths().get(i).getFullPath());
    }
    Assert.assertEquals(deduplicatedPaths.length, plan.getDeduplicatedDataTypes().size());
    for (Map.Entry<String, Integer> entry : plan.getPathToIndex().entrySet()) {
      Assert.assertEquals(pathToIndex.get(entry.getKey()), entry.getValue());
    }
    Assert.assertEquals(deviceToMeasurements.get("root.sg.d1").size(), plan.getAllMeasurementsInDevice("root.sg.d1").size());
    Assert.assertEquals(deviceToMeasurements.get("root.sg.d2").size(), plan.getAllMeasurementsInDevice("root.sg.d2").size());
    Assert.assertArrayEquals(deduplicatedAggregations, plan.getDeduplicatedAggregations().toArray(new String[0]));
  }

  @Test
  public void testLastQueryPlanDeduplicate() throws MetadataException {
    deduplicatedPaths = new Path[] {
        paths[0], paths[1], paths[3], paths[5], paths[6]
    };

    int i = 0;
    for (Path path : deduplicatedPaths) {
      pathToIndex.put(path.getFullPath(), i++);
    }

    LastQueryPlan plan = new LastQueryPlan();
    plan.setAlignByTime(true);
    plan.setOperatorType(OperatorType.QUERY);
    plan.setPaths(Arrays.asList(paths));

    generator.deduplicate(plan);
    for (i = 0; i < deduplicatedPaths.length; i ++) {
      Assert.assertEquals(deduplicatedPaths[i].getFullPath(), plan.getDeduplicatedPaths().get(i).getFullPath());
    }
    Assert.assertEquals(deduplicatedPaths.length, plan.getDeduplicatedDataTypes().size());
  }

  @Mock
  QueryOperator operator;
  @Mock
  FromOperator fromOperator;
  @Test
  public void testAlignByDevicePlanDeduplicate() throws QueryProcessException {


    SelectOperator selectOperator = new SelectOperator(SQLConstant.TOK_SELECT);
    selectOperator.setSuffixPathList(Arrays.asList(new Path("s1"), new Path("s2"), new Path("s1"), new Path("1")));

    //operator = mock(QueryOperator.class);
    when(operator.getFromOperator()).thenReturn(fromOperator);
    when(operator.getSelectOperator()).thenReturn(selectOperator);
    when(operator.getFilterOperator()).thenReturn(null);
    when(operator.hasSlimit()).thenReturn(false);
    when(operator.getFromOperator().getPrefixPaths()).thenReturn(Arrays.asList(new Path("root.sg.d1"), new Path("root.sg.d2")));

    RawDataQueryPlan plan = new RawDataQueryPlan();
    AlignByDevicePlan alignByDevicePlan = generator.deduplicate(operator, plan);

    Assert.assertArrayEquals( new String[]{"s1","s2","s1","1"}, alignByDevicePlan.getMeasurements().toArray(new String[0]));
    Assert.assertArrayEquals( new String[]{"root.sg.d1","root.sg.d2"}, alignByDevicePlan.getDevices().toArray(new String[0]));
    Map<String, TSDataType> measurementDataTypeMap = new HashMap<>();
    measurementDataTypeMap.put("s1", TSDataType.INT32);
    measurementDataTypeMap.put("s2", TSDataType.INT32);
    Map<String, MeasurementType> measurementTypeMap = new HashMap<>();
    measurementTypeMap.put("s1", MeasurementType.Exist);
    measurementTypeMap.put("s2", MeasurementType.Exist);
    measurementTypeMap.put("1", MeasurementType.NonExist);

    Assert.assertEquals(measurementDataTypeMap, alignByDevicePlan.getMeasurementDataTypeMap());
    Assert.assertEquals(measurementTypeMap, alignByDevicePlan.getMeasurementTypeMap());

  }
}
