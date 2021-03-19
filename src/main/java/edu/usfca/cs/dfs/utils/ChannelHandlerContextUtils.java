package edu.usfca.cs.dfs.utils;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;

public class ChannelHandlerContextUtils {

    public static String getIpAddressFromChannel(Channel channel) {
        return ((InetSocketAddress) channel.remoteAddress()).getAddress().getHostAddress();
    }

    public static int getPortFromChannel(Channel channel) {
        return ((InetSocketAddress) channel.remoteAddress()).getPort();
    }

    public static String getRemoteAddressWithPort(Channel channel) {
        return getIpAddressFromChannel(channel) + ":" + getPortFromChannel(channel);
    }

    public static void main(String[] args) {
        System.out.println("Hello world");
    }
}
