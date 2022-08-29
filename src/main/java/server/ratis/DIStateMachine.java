package server.ratis;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DIStateMachine extends BaseStateMachine {

    private final DIMetadata metadata;

    private SimpleStateMachineStorage smStorage = new SimpleStateMachineStorage();

    DIStateMachine(DIMetadata md) {
        metadata = md;
    }

    void print(Object o) { System.out.println(new Date() + "|" + o);}

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

        for(String line: contents) {
            String[] kv = line.split("_");
            wordFreqMap.put(kv[0], Integer.parseInt(kv[1]));
        }

        synchronized (metadata) {
            metadata.updateFrequencyMapAndTermIndex(wordFreqMap, termIndex);
            updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex());
        }

    }

    @Override //raftStorage is a file system directory - basically physical storage + configuration
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
        super.initialize(raftServer, raftGroupId, raftStorage);
        smStorage.init(raftStorage);
        //load the snapshot from storage into state machine
        loadSnapshotFromStorage(smStorage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException { // sm in pause state comes here
        loadSnapshotFromStorage(smStorage.getLatestSnapshot()); // reloads latest persisted snapshot with edits
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
}
