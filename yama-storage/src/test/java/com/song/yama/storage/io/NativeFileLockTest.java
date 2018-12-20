/*
 *  Copyright 2018 acoder2013
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http:www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.song.yama.storage.io;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class NativeFileLockTest {

    @Test
    public void lock() throws IOException, InterruptedException {
        File tempFile = File.createTempFile(NativeFileLockTest.class.getSimpleName(), ".lock");
        new Thread(() -> {
            try {
                NativeFileLock.lock(tempFile.getAbsolutePath(), () -> {
                    System.out.println("Get Lock");
                    try {
                        Thread.sleep(200L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(100);
        new Thread(() -> {
            try {
                NativeFileLock.lock(tempFile.getAbsolutePath(), () -> {
                    fail("Shouldn't get the lock");
                });
            } catch (Exception e) {
                //expected behaviour
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                NativeFileLock.lock(tempFile.getAbsolutePath(), () -> {
                    System.out.println("Get lock!");
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);
    }
}