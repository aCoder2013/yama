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

package com.song.yama.common.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;

public class ThreadUtils {

    public static ThreadFactory newThreadFactory(String nameFormat, Logger logger) {
        return new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setUncaughtExceptionHandler((t, e) -> logger.error("Uncaught exception :" + t.getName(), e))
            .build();
    }

}
