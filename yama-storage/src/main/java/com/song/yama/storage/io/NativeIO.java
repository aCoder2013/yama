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

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

/**
 * A native io utility class
 */
public class NativeIO {

    static {
        Native.register("c");
    }

    public native Pointer fopen(String filename, String mode) throws LastErrorException;

    public native int flock(int fd, int operation) throws LastErrorException;

    public native int fileno(Pointer fp);

    public native int fclose(Pointer fp);
}
