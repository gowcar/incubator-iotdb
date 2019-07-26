/**
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
package org.apache.iotdb.tsfile;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * The class is to show how to read TsFile file named "test.tsfile".
 * The TsFile file "test.tsfile" is generated from class TsFileWrite.
 * Run TsFileWrite to generate the test.tsfile first
 */
public class TsFileRead {
  private static void queryAndPrint(ArrayList<Path> paths, ReadOnlyTsFile readTsFile, IExpression statement)
          throws IOException {
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    while (queryDataSet.hasNext()) {
      System.out.println(queryDataSet.next());
    }
    System.out.println("------------");
  }

  public static void main(String[] args) throws IOException {

    // file path
    String path = "test.tsfile";

    // create reader and get the readTsFile interface
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    // use these paths(all sensors) for all the queries
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_1.sensor_2"));
    paths.add(new Path("device_1.sensor_3"));

    // no filter, should select 1 2 3 4 6 7 8
    queryAndPrint(paths, readTsFile, null);

    // time filter : 4 <= time <= 10, should select 4 6 7 8
    IExpression timeFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(4L)),
            new GlobalTimeExpression(TimeFilter.ltEq(10L)));
    queryAndPrint(paths, readTsFile, timeFilter);

    // value filter : device_1.sensor_2 <= 20, should select 1 2 4 6 7
    IExpression valueFilter = new SingleSeriesExpression(new Path("device_1.sensor_2"),
            ValueFilter.ltEq(20));
    queryAndPrint(paths, readTsFile, valueFilter);

    // time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20, should select 4 7 8
    timeFilter = BinaryExpression.and(new GlobalTimeExpression(TimeFilter.gtEq(4L)),
            new GlobalTimeExpression(TimeFilter.ltEq(10L)));
    valueFilter = new SingleSeriesExpression(new Path("device_1.sensor_3"), ValueFilter.gtEq(20));
    IExpression finalFilter = BinaryExpression.and(timeFilter, valueFilter);
    queryAndPrint(paths, readTsFile, finalFilter);

    //close the reader when you left
    reader.close();
  }
}