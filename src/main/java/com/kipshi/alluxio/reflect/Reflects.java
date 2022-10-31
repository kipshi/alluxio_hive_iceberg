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

package com.kipshi.alluxio.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class Reflects {

    public static class Test {

        public Test() {

        }

        private final String a = "a";

        private final String b = "b";

        public String getA() {
            return a;
        }

        public String getB() {
            return b;
        }
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        Test test = new Test();
        System.out.println(test.getA());
        Field aField = test.getClass().getDeclaredField("a");
        aField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(aField, aField.getModifiers() & ~Modifier.FINAL);
        System.out.println(aField.get(test));
        System.out.println(test.a);
        aField.set(test,"aa");
        System.out.println(aField.get(test));
        aField.set(test,"aaa");
        System.out.println(aField.get(test));
        Thread.sleep(10);
        System.out.println(test.getA().equals("aaa"));

    }

}
