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

package com.song.yama.messaging;

import com.google.common.net.HostAndPort;
import com.song.yama.messaging.exception.IllegalAddressException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.Data;

@Data
public class Address {

    private final String host;

    private final int port;

    private final InetAddress inetAddress;

    public Address(String host, int port, InetAddress inetAddress) {
        this.host = host;
        this.port = port;
        this.inetAddress = inetAddress;
    }

    public static Address fromString(String address) {
        HostAndPort hostAndPort = HostAndPort.fromString(address);
        try {
            return new Address(hostAndPort.getHost(), hostAndPort.getPort(),
                InetAddress.getByName(hostAndPort.getHost()));
        } catch (UnknownHostException e) {
            throw new IllegalAddressException(address, e);
        }
    }

    public static Address fromHostAndPort(String host, int port) {
        try {
            return new Address(host, port, InetAddress.getByName(host));
        } catch (UnknownHostException e) {
            throw new IllegalAddressException(host, e);
        }
    }
}
