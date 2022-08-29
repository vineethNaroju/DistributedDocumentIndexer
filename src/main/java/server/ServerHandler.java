package server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import server.ratis.DIRaftServer;

import java.util.Date;
import java.util.Objects;

public class ServerHandler extends SimpleChannelInboundHandler<String> {

//    private static final List<Channel> channels = new ArrayList<>();

    private final DIRaftServer raftServer;
    private final String INSERT = "insert", FREQUENCY = "frequency", LEADER = "leader";

    ServerHandler(DIRaftServer raftServer) {
        this.raftServer = raftServer;
    }

    public static void println(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) {
        println("[" + ctx.channel().remoteAddress()+ "]:" + s);

        String[] cmd = s.split(",");

        String response = "none";

        switch (cmd[0]) {
            case INSERT:
                response = raftServer.insertDocument(cmd[1]);
                break;
            case FREQUENCY:
                response = String.valueOf(raftServer.getSingleWordFrequency(cmd[1]));
                break;
            case LEADER:
                response = raftServer.isLeaderAndReady() ?
                        "server is leader and ready" : "server is (not leader) or (leader but no ready)";
                break;
        }

        ctx.channel().writeAndFlush(response + "\n");
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
