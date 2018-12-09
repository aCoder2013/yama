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

import lombok.Data;

@Data
public class Result<T> {

    private boolean success;

    private int code;

    private T data;

    private String message;

    private Throwable throwable;

    public static <T> Result<T> success() {
        return new Result<>(true);
    }

    public static <T> Result<T> success(T data) {
        return new Result<>(true, data);
    }

    public static <T> Result<T> fail() {
        return new Result<>(false);
    }

    public static <T> Result<T> fail(T data) {
        return new Result<>(false, data);
    }

    public static <T> Result<T> fail(String message) {
        return new Result<>(false, -1, message, null);
    }

    public static <T> Result<T> fail(int code, String message) {
        return new Result<>(false, code, message, null);
    }

    public static <T> Result<T> fail(int code, String message,T data) {
        return new Result<>(false, code, message, null, data);
    }

    public static <T> Result<T> fail(Throwable throwable) {
        return new Result<>(false, -1, throwable.getMessage(), throwable);
    }

    public static <T> Result<T> fail(int code, Throwable throwable) {
        return new Result<>(false, code, throwable.getMessage(), throwable);
    }

    public Result(boolean success) {
        this.success = success;
    }

    public Result(boolean success, T data) {
        this.success = success;
        this.data = data;
    }

    public Result(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public Result(boolean success, String message, Throwable throwable) {
        this.success = success;
        this.message = message;
        this.throwable = throwable;
    }

    public Result(boolean success, int code, String message, Throwable throwable) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.throwable = throwable;
    }

    public Result(boolean success, int code, String message, Throwable throwable,T data) {
        this.success = success;
        this.code = code;
        this.message = message;
        this.throwable = throwable;
        this.data = data;
    }

    public boolean isFailure() {
        return !isSuccess();
    }
}
