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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class NativeFileLock {

    /**
     * Attempts to acquire an exclusive file lock
     *
     * @param path file path
     * @param runnable code snippet to run when acquired the lock
     */
    public static void lock(String path, Runnable runnable) throws IOException {
        RandomAccessFile randomAccessFile = null;
        FileChannel channel = null;
        FileLock fileLock = null;
        try {
            randomAccessFile = new RandomAccessFile(new File(path), "rw");
            channel = randomAccessFile.getChannel();
            fileLock = channel.tryLock();
            if (fileLock == null) {
                throw new IOException("Lock could not be acquired because another program holds an overlapping lock");
            } else {
                runnable.run();
                fileLock.release();
            }
        } finally {
            if (fileLock != null) {
                fileLock.close();
            }
            if (channel != null) {
                channel.close();
            }

            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        }
    }

}
