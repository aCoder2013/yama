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

package com.song.yama.example.raft.controller;

import com.song.yama.example.raft.core.StateMachine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
@RequestMapping(value = "/yama/raft/api/v1")
public class ApiController {

    @Autowired
    private StateMachine stateMachine;

    @GetMapping("/get")
    public @ResponseBody
    String get(String key) {
        return stateMachine.lookup(key);
    }

    @GetMapping("/put")
    public @ResponseBody
    String put(@RequestParam("key") String key, @RequestParam("value") String value) {
        this.stateMachine.propose(key, value);
        return "ok";
    }

}
