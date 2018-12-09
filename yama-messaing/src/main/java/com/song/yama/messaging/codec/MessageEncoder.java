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

package com.song.yama.messaging.codec;

import com.song.yama.messaging.message.RemotingMessage;
import com.song.yama.messaging.utils.RemotingUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageEncoder extends MessageToByteEncoder<RemotingMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingMessage msg, ByteBuf out) throws Exception {
        try {
            out.writeBytes(msg.encode());
        } catch (Exception e) {
            log.error(RemotingUtils.parseChannelAddress(ctx.channel()) + ",Encoding message failed :" + msg.toString(),
                e);
            RemotingUtils.closeChannel(ctx.channel());
        }
    }
}
