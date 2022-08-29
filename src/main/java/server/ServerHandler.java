package server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import server.ratis.DIRaftServer;

import java.util.Date;

public class ServerHandler extends SimpleChannelInboundHandler<String> {

//    private static final List<Channel> channels = new ArrayList<>();

    private final DIRaftServer raftServer;

    ServerHandler(DIRaftServer raftServer) {
        this.raftServer = raftServer;
    }

    public static void println(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) {
        println("[" + ctx.channel().remoteAddress()+ "]:" + s);
        ctx.channel().writeAndFlush("raft server status:" + raftServer.checkStatus() + "\n");
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)  {
        println(ctx.channel() + " is added");
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        println(ctx.channel() + " is removed");
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }
}
