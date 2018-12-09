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

package com.song.yama.messaging.utils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import java.net.SocketAddress;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemotingUtils {

    public static String parseChannelAddress(final Channel channel) {
        if (null == channel) {
            return "";
        }
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    public static void closeChannel(Channel channel) {
        String channelInfo = channel.toString();
        channel.close().addListener((ChannelFutureListener) future ->
            log.info("Close the connection to remote address[{}] result: {}", channelInfo,
                future.isSuccess()));
    }
}
