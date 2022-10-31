/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kipshi.alluxio;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.shaded.client.org.apache.commons.io.IOUtils;
import alluxio.wire.SyncPointInfo;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

public class AlluxioConnect {

    public static final String ALLUXIO_MASTER = "43.143.76.135";
    public static final Integer ALLUXIO_PORT = 30603;
    public static final String USER = "alluxio";
    public static final String PATH = "/hello";
    public static final List<String> JOURNAL_ADDRESS = Lists.newArrayList(
            "43.143.76.135:30604");


    public static void main(String[] args) throws IOException, AlluxioException {
        Configuration.set(PropertyKey.MASTER_HOSTNAME, ALLUXIO_MASTER);
        Configuration.set(PropertyKey.MASTER_RPC_PORT, ALLUXIO_PORT);
        Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, JOURNAL_ADDRESS);
        FileSystem fs = FileSystem.Factory.get();

        AlluxioURI path = new AlluxioURI("/myFile");
// Create a file and get its output stream
        FileOutStream out = fs.createFile(path);
        List<SyncPointInfo> syncPointInfos = fs.getSyncPathList();
        for (SyncPointInfo syncPointInfo : syncPointInfos) {
            System.out.println(syncPointInfo.getSyncPointUri().toString());
        }
        try (FileInStream in = fs.openFile(new AlluxioURI(PATH))) {
            String content = IOUtils.toString(in, "utf-8");
            System.out.println(content);
        } catch (AlluxioException | IOException e) {
            e.printStackTrace();
        }

    }

}
