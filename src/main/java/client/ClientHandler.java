package client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

public class ClientHandler extends SimpleChannelInboundHandler<String> {

    public static void println(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) {
        println("[" + ctx.channel().remoteAddress() +"]:" + s);
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        println("Closing connection for " + ctx + " due to " + cause);
        ctx.close();
    }
}
