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

package com.song.yama.storage.utils;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IOUtils {

    public static List<String> listNames(File file, String suffix) {
        if (file != null && file.isDirectory()) {
            String[] list = file.list((dir, name) -> name.endsWith(suffix));
            if (list != null) {
                return Lists.newArrayList(list);
            }
        }
        return Collections.emptyList();
    }

    public static void ensureDirOK(String dir) {
        if (dir != null && !dir.equals("")) {
            File file = new File(dir);
            if (file.isDirectory()) {
                if (!file.exists()) {
                    boolean result = file.mkdir();
                    log.info("Create dir [{}] result : {}.", dir, result);
                }
            }
        }
    }
}
