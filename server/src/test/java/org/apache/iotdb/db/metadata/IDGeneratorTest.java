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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.junit.Assert;
import org.junit.Test;

public class IDGeneratorTest {

  @Test
  public void testNewGenerator() throws MetadataException {
    Assert.assertEquals(1, IDGenerator.newSGNumber());
    Assert.assertEquals(1, IDGenerator.newDeviceNumber());
    Assert.assertEquals(1, IDGenerator.newMeasurementNumber());
    //0000 0000 0000 0010 0000 0000 0000 0000 0000 0000 0000 0010 0000 0000 0000 0010
    Assert.assertEquals(0x0002000000020002L, IDGenerator.newID());
  }

}
