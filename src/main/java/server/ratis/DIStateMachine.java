package server.ratis;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class DIStateMachine extends BaseStateMachine {

    private final DIMetadata metadata;

    private SimpleStateMachineStorage smStorage = new SimpleStateMachineStorage();

    DIStateMachine(DIMetadata md) {
        metadata = md;
    }

    void print(Object o) { System.out.println(new Date() + "|" + o);}

    // update the metadata & termindex atomically from local disk storage
    private void loadSnapshotFromStorage(SingleFileSnapshotInfo snapshotInfo) throws IOException {

        if(snapshotInfo == null) {
            print("snapshotInfo is null");
            return;
        }

        final Path path = snapshotInfo.getFile().getPath();

        if(!Files.exists(path)) {
            print("path[" + path + "]does not exist");
            return;
        }

        final TermIndex termIndex = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(path.toFile());

        List<String> contents = Files.readAllLines(path);

        final Map<String, Integer> wordFreqMap = new HashMap<>();

        // expensive operation incase of huge snapshot size.
        for(String line: contents) {
            String[] kv = line.split("_");
            wordFreqMap.put(kv[0], Integer.parseInt(kv[1]));
        }

        // only one metadata object will exist in this usecase.
        synchronized (metadata) {
            metadata.updateFrequencyMapAndTermIndex(wordFreqMap, termIndex);
            updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex());
        }

    }


    // RaftStorage is an abstraction over local disk to store ephermal raft server data like
    // configuration, snapshots, etc and also a file system directory.
    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
        super.initialize(raftServer, raftGroupId, raftStorage);
        smStorage.init(raftStorage); // loads from local disk into raft server storage abstraction.
        //load the snapshot from storage into state machine
        loadSnapshotFromStorage(smStorage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException { // sm in pause state comes here
        loadSnapshotFromStorage(smStorage.getLatestSnapshot());
        // reloads latest persisted snapshot with edits
    }

    @Override
    public long takeSnapshot() throws IOException {

        TermIndex termIndex;
        List<String> formattedWordFrequencies;

        synchronized (metadata) {
            TermIndex temp = metadata.getLatestAppliedTermIndex();
            termIndex = TermIndex.valueOf(temp.getTerm(), temp.getIndex());
            formattedWordFrequencies = metadata.getAllWordFrequencies();
        }

        final File snapshotFile = smStorage.getSnapshotFile(termIndex.getTerm(), termIndex.getIndex());

        try (OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(snapshotFile.toPath()))) {

            for(String entry : formattedWordFrequencies) {
                String line = entry + "\n";
                outputStream.write(line.getBytes(StandardCharsets.UTF_8));
            }

        } catch (Exception e) {
            e.printStackTrace();
            return RaftLog.INVALID_LOG_INDEX;
        }

        return termIndex.getIndex();
    }

    // committed raft log entry i.e, log entry is appended to majority of raft server's log.
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

        RaftProtos.LogEntryProto logEntry = trx.getLogEntry();
        String document = logEntry.getStateMachineLogEntry().getLogData().toStringUtf8();
        TermIndex termIndex = TermIndex.valueOf(logEntry);

        synchronized (metadata) {
            metadata.insertDocumentAndUpdateTermIndex(document, termIndex);
            updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex());
        }

        return CompletableFuture.completedFuture(Message.valueOf("success"));
    }

    // EXPLORING




    //StateMachine
    //Validate/pre-process the incoming update request in the state machine.
    //Returns:
    //the content to be written to the log entry. Null means the request should be rejected.
    //Throws:
    //IOException – thrown by the state machine while validation
    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        return super.startTransaction(request);
    }

    //StateMachine
    //This is called before the transaction passed from the StateMachine is appended to the raft log.
    // This method will be called from log append and having the same strict serial order that the transactions will have in the RAFT log.
    // Since this is called serially in the critical path of log append, it is important to do only required operations here.
    //Returns: The Transaction context.
    @Override
    public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
        return super.preAppendTransaction(trx);
    }

    //StateMachine
    //Called to notify the state machine that the Transaction passed cannot be appended (or synced).
    // The exception field will indicate whether there was an exception or not.
    //Params:
    //trx – the transaction to cancel
    //Returns:
    //cancelled transaction
    @Override
    public TransactionContext cancelTransaction(TransactionContext trx) throws IOException {
        return super.cancelTransaction(trx);
    }

    //EventApi
    //Notify the StateMachine a term-index update event. This method will be invoked when a RaftProtos.
    // MetadataProto or RaftProtos.RaftConfigurationProto is processed. For RaftProtos.StateMachineLogEntryProto, this method will not be invoked.
    //Params:
    //term – The term of the log entry index – The index of the log entry
    @Override
    public void notifyTermIndexUpdated(long term, long index) {
        super.notifyTermIndexUpdated(term, index);
    }

    //EventApi
    //Notify the StateMachine that a new leader has been elected. Note that the new leader can possibly be this server.
    //Params:
    //groupMemberId – The id of this server. newLeaderId – The id of the new leader
    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        super.notifyLeaderChanged(groupMemberId, newLeaderId);
    }

    //LeaderEventApi - be invoked only when the server is a leader.
    //Notify StateMachine that this server is no longer the leader.
    @Override
    public void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException {
        super.notifyNotLeader(pendingEntries);
    }

    //FollowerEventApi - be invoked only when the server is a follower.
    //Notify the StateMachine that there is no leader in the group for an extended period of time.
    // This notification is based on "raft.server.notification.no-leader.timeout".
    //Params:
    //roleInfoProto – information about the current node role and rpc delay information
    //See Also:
    //org.apache.ratis.server.RaftServerConfigKeys.Notification.NO_LEADER_TIMEOUT_KEY
    @Override
    public void notifyExtendedNoLeader(RaftProtos.RoleInfoProto roleInfoProto) {
        super.notifyExtendedNoLeader(roleInfoProto);
    }

    //FollowerEventApi - be invoked only when the server is a follower.
    //Notify the StateMachine that the leader has purged entries from its log.
    // In order to catch up, the StateMachine has to install the latest snapshot asynchronously.
    //Params:
    //roleInfoProto – information about the current node role and rpc delay information. firstTermIndexInLog – The term-index of the first append entry available in the leader's log.
    //Returns:
    //return the last term-index in the snapshot after the snapshot installation.
    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
        return super.notifyInstallSnapshotFromLeader(roleInfoProto, firstTermIndexInLog);
    }


}
