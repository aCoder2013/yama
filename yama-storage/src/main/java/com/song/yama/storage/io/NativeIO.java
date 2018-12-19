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

import com.google.common.io.Files;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.IOException;

/**
 * A native io utility class
 */
public class NativeIO {

    /*
     * The values of these constants are defined in the flock API and thus
     * should be portable.
     */


    private static final int LOCK_SH = 1;   /* shared lock */

    private static final int LOCK_EX = 2;   /* exclusive lock */

    private static final int LOCK_NB = 4;    /* don't block when locking */

    private static final int LOCK_UN = 8;    /* unlock */

    private static final String READ_MODE = "r";

    private static final String WRITE_MODE = "w";

    private static final String READ_WRITE_MODE = "rw";

    static {
        Native.register("c");
    }

    public static synchronized void lock(String path) throws IOException {
        Files.createParentDirs(new File(path));
        Pointer fp = null;
        try {
            fp = fopen(path, WRITE_MODE);
            flock(fileno(fp), LOCK_EX | LOCK_NB);
        } catch (LastErrorException e) {
            throw new IOException("Failed to acquire file lock :" + e.getMessage(), e);
        } finally {
            if (fp != null) {
                fclose(fp);
            }
        }
    }

    public static synchronized void unlock(String path) throws IOException {
        Pointer fp = null;
        try {
            fp = fopen(path, WRITE_MODE);
            flock(fileno(fp), LOCK_UN);
        } catch (LastErrorException e) {
            throw new IOException("Failed to release file lock:" + e.getMessage(), e);
        } finally {
            if (fp != null) {
                fclose(fp);
            }
        }
    }

    public static native Pointer fopen(String filename, String mode) throws LastErrorException;

    public static native int flock(int fd, int operation) throws LastErrorException;

    public static native int fileno(Pointer fp);

    public static native int fclose(Pointer fp);
}
