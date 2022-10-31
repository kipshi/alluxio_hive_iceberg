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

package com.kipshi.alluxio.iceberg;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hive.HiveCatalog;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class IcebergClient {

    public static HiveCatalog create() throws Exception {
        HiveCatalog catalog = new HiveCatalog();
//        HiveConf hiveConf = new HiveConf();
//        hiveConf.addResource("hive-site.xml");
        System.out.println("hadoop conf:" + getConf().get("fs.alluxio.impl"));
        catalog.setConf(getConf());

        URI uri = URI.create("alluxio://172.17.0.171:19998/");
        FileSystem fileSystem = FileSystem.get(uri, getConf());
        Path path = new Path("/hello");
        System.out.println("file exists:" + fileSystem.exists(path));

        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", "alluxio://172.17.0.66:19998,172.17.0.171:19998/iceberg");
        properties.put("uri", "thrift://tbds-172-17-1-118:9083,thrift://tbds-172-17-1-25:9083");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
        catalog.initialize("hive", properties);
        return catalog;
    }

    private static org.apache.hadoop.conf.Configuration getConf() throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        conf.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
        //alluxio.security.kerberos.client.principal=alluxio/tbds.instance@TBDS.COM
        //alluxio.security.kerberos.client.keytab.file=/Users/guoyuchen/project/alluxioExemple/alluxio.keytab
        conf.set("alluxio.security.kerberos.client.principal", "alluxio/tbds.instance@TBDS.COM");
        conf.set("alluxio.security.kerberos.client.keytab.file", "/etc/security/keytabs/alluxio.keytab");
        conf.set("alluxio.security.login.impersonation.username", "_NONE_");
        return conf;
    }
}
