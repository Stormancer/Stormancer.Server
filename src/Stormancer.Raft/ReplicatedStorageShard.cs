using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Stormancer.Raft;
using Stormancer.Threading;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Stormancer.Raft
{


    public class RaftCommand
    {
        public Guid Id { get; } = Guid.NewGuid();


        public IRecord Record { get; }

        private RaftCommand(IRecord record)
        {
            Record = record;
        }

        public static RaftCommand Create(IRecord record)
        {
            return new RaftCommand(record);
        }

        public TSystemCommandContent? As<TSystemCommandContent>() where TSystemCommandContent : class, IRecord<TSystemCommandContent>
        {
            return Record as TSystemCommandContent;
        }

        public int GetLength(IRecordReaderWriter readerWriter)
        {
            return 16 + readerWriter.GetContentLength(Record);
        }

        public bool TryWrite(Span<byte> buffer, out int bytesWritten, IRecordReaderWriter readerWriter)
        {
            var written = 0;
            var result = Id.TryWriteBytes(buffer) && readerWriter.TryWriteContent(buffer.Slice(16), Record, out written);
            if (result)
            {
                bytesWritten = written + 16;
                return result;
            }
            else
            {
                bytesWritten = 0;
                return false;
            }
        }


    }


    public class RaftCommandResult
    {
        public Guid OperationId { get; init; }

        [MemberNotNullWhen(false, "Error")]
        public bool Success => Error == null;

        public Error? Error { get; init; }

        public ulong LogEntryId { get; init; }
        public ulong Term { get; init; }

        public int GetLength()
        {
            return 16 + 8 + 8 + 1 + (Success ? 0 : Error.GetLength());
        }

        public bool TryWrite(Span<byte> span, out int bytesWritten)
        {
            if (span.Length < 33)
            {
                bytesWritten = 0;
                return false;
            }
            var result = OperationId.TryWriteBytes(span) &&
                BinaryPrimitives.TryWriteUInt64BigEndian(span.Slice(16), Term) &&
                BinaryPrimitives.TryWriteUInt64BigEndian(span.Slice(24), LogEntryId);

            if (!result)
            {
                bytesWritten = 0;
                return false;
            }
            span[32] = Success ? (byte)1 : (byte)0;
            if (Success)
            {
                bytesWritten = 33;
                return true;
            }
            else
            {
                if (Error.TryWrite(span.Slice(33), out var written))
                {
                    bytesWritten = 33 + written;
                    return true;
                }
                else
                {
                    bytesWritten = 0;
                    return false;
                }
            }
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, out int bytesRead, [NotNullWhen(true)] out RaftCommandResult? cmdResult)
        {
            var reader = new SequenceReader<byte>(buffer);

            Span<byte> b = stackalloc byte[33];

            var result = reader.TryCopyTo(b);

            if (!result)
            {
                bytesRead = (int)buffer.Length;
                cmdResult = null;
                return false;
            }

            bool success = b[32] != 0;


            var term = BinaryPrimitives.ReadUInt64BigEndian(b.Slice(16));
            var entryId = BinaryPrimitives.ReadUInt64BigEndian(b.Slice(24));
            if (success)
            {
                if (Error.TryRead(buffer.Slice(33), out var error, out var read))
                {
                    bytesRead = read + 33;
                    cmdResult = new RaftCommandResult { OperationId = new Guid(b.Slice(0, 16)), Error = error, Term = term, LogEntryId = entryId };
                    return true;
                }
                else
                {
                    bytesRead = read + 33;
                    cmdResult = null;
                    return false;
                }
            }
            else
            {
                bytesRead = 33;
                cmdResult = new RaftCommandResult { OperationId = new Guid(b.Slice(0, 16)), Error = null, Term = term, LogEntryId = entryId };
                return true;
            }


        }


        internal static RaftCommandResult CreateFailed(Guid id, Error error)
        {
            return new RaftCommandResult { Error = error, OperationId = id };
        }
    }










    public class ReplicatedStorageShard : IReplicatedStorageMessageHandler
    {
        private class ShardReplicaSynchronisationState(Guid shardUid, ulong nextLogEntryIdToSend)
        {
            public Guid ShardUid { get; set; } = shardUid;

            public ulong NextLogEntryIdToSend { get; set; } = nextLogEntryIdToSend;

            public ulong LastKnownReplicatedLogEntry { get; set; } = 0;

            public DateTime LastAppendEntriesSentOn { get; set; } = DateTime.MinValue;
            public bool LastAppendSuccess { get; internal set; } = true;
            public bool AppendInProgress { get; internal set; } = false;
            public ulong LastAppliedLogEntryId { get; internal set; }

            public object SyncRoot = new object();
        }

        private readonly Dictionary<Guid, ShardReplicaSynchronisationState> _shardInstances = new Dictionary<Guid, ShardReplicaSynchronisationState>();


        private readonly object _syncRoot = new object();

        private readonly ILogger _logger;
        private ulong _commitIndex = 0;
        private Guid? _votedFor = null;

        private readonly IReplicatedStoreMessageChannel? _channel;
        private readonly IStorageShardBackend _backend;

        private readonly ReplicatedStorageShardConfiguration _config;

        public Guid ShardUid { get; }

        public Guid? LeaderUid { get; private set; }


        public bool IsLeader => LeaderUid == ShardUid;

        public ReplicatedStorageShard(Guid shardUid,
            ReplicatedStorageShardConfiguration config,
            ILoggerFactory logger,
            IReplicatedStoreMessageChannel? channel,
            IStorageShardBackend backend
            )
        {
            ArgumentNullException.ThrowIfNull(config, nameof(config));
            _config = config;
            ShardUid = shardUid;

            _channel = channel;
            _backend = backend;
            _logger = logger.CreateLogger(ShardUid.ToString());

        }



        public RaftCommandResult ExecuteCommand(RaftCommand command)
        {
            ShardsReplicationLogging.LogStartProcessingCommand(_logger, command.Id, command.Record.GetType());

            if (IsLeader || _backend.CurrentShardsConfiguration.New == null)
            {
                var result = _backend.TryAppendCommand(command);
                if (result.Success)
                {

                    if (!AppendEntriesToReplica(false))
                    {
                        //no replica, we should commit immediately.
                        TryCommit();

                    }
                }

                return result;


            }
            else
            {
                return new RaftCommandResult { Error = new Error(RaftErrors.NotLEader, null), OperationId = command.Id };
            }


        }
        public ValueTask<bool> WaitCommitted(RaftCommandResult result) => WaitCommitted(result.Term, result.LogEntryId);
        public ValueTask<bool> WaitCommitted(ulong term, ulong entryId)
        {

            if (_backend.CurrentTerm != term)
            {
                if (_backend.TryGetEntryTerm(entryId, out var entryTerm))
                {
                    return ValueTask.FromResult(false);
                }
                else
                {
                    return ValueTask.FromResult(entryTerm == term);
                }
            }
            else
            {
                if (_backend.LastAppliedLogEntry >= entryId)
                {
                    return ValueTask.FromResult(true);
                }
                else
                {
                    lock (_pendingOperationsLock)
                    {
                        var op = new AsyncOperationWithData<(ulong term, ulong entryId), bool>(true);
                        op.Item = (term, entryId);
                        if (_lastPendingOperation == null)
                        {
                            _firstPendingOperation = op;
                            _lastPendingOperation = op;
                        }
                        else
                        {
                            _lastPendingOperation.Next = op;
                            op = _lastPendingOperation;
                        }
                        return op.ValueTaskOfT;
                    }
                }
            }

        }

        private void ProcessCompletedOperations()
        {
            lock (_pendingOperationsLock)
            {
                var lastAppliedCommit = _backend.LastAppliedLogEntry;

                foreach (var state in _shardInstances)
                {
                    if (state.Value.LastAppliedLogEntryId < lastAppliedCommit)
                    {
                        lastAppliedCommit = state.Value.LastAppliedLogEntryId;
                    }
                }
                if(lastAppliedCommit == 0)
                {
                    return;
                }
                _logger.Log(LogLevel.Trace, "Processing completed operations. commit={lastAppliedCommit}[{term}]", lastAppliedCommit, _backend.CurrentTerm);
                var current = _firstPendingOperation;
                while (current != null)
                {
                    if (current.Item.term != _backend.CurrentTerm)
                    {
                        current.TrySetResult(false);
                        _firstPendingOperation = (AsyncOperationWithData<(ulong term, ulong entryId), bool>?)current.Next;
                        current = _firstPendingOperation;

                    }
                    else if (current.Item.entryId <= lastAppliedCommit)
                    {
                        current.TrySetResult(true);
                        _firstPendingOperation = (AsyncOperationWithData<(ulong term, ulong entryId), bool>?)current.Next;
                        current = _firstPendingOperation;
                    }
                    else
                    {
                        current = null;
                    }

                }
            }
        }
        private readonly object _pendingOperationsLock = new object();
        private AsyncOperationWithData<(ulong term, ulong entryId), bool>? _firstPendingOperation;
        private AsyncOperationWithData<(ulong term, ulong entryId), bool>? _lastPendingOperation;


        private RaftCommandResult ExecuteNoOp()
        {

            return ExecuteCommand(RaftCommand.Create(NoOpRecord.Instance));
        }

        public async ValueTask UpdateClusterConfiguration(IEnumerable<Server> newConfiguration)
        {
            if (_backend.CurrentShardsConfiguration.Old != null)
            {
                throw new InvalidOperationException("Cannot update the cluster configuration while another configuration is pending.");
            }

            if (_backend.CurrentShardsConfiguration.New != null && IsLeader == false)
            {
                throw new InvalidOperationException("Only the leader can update the cluster configuration.");
            }


            var result = UpdateClusterConfiguration(new ShardsConfigurationRecord(_backend.CurrentShardsConfiguration.New, [.. newConfiguration]));
            await WaitCommitted(result);

            if (!result.Success)
            {
                throw new InvalidOperationException($"Failed to update cluster configuration : {result.Error}");
            }

            if (_backend.CurrentShardsConfiguration.Old != null)
            {
                result = UpdateClusterConfiguration(new ShardsConfigurationRecord(null, [.. newConfiguration]));
                await WaitCommitted(result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Failed to update cluster configuration : {result.Error}");
                }
            }

            if (!IsShardVoting(ShardUid))
            {
                StepDownLeadership();
            }
        }

        private RaftCommandResult UpdateClusterConfiguration(ShardsConfigurationRecord record)
        {
            return ExecuteCommand(RaftCommand.Create(record));

        }

        private Dictionary<Guid, AsyncOperation<RaftCommandResult>> _forwardedCommands = new Dictionary<Guid, AsyncOperation<RaftCommandResult>>();

        private ObjectPool<AsyncOperation<RaftCommandResult>> _operationPool = new DefaultObjectPool<AsyncOperation<RaftCommandResult>>(new AsyncOperationPoolPolicy<RaftCommandResult>());


        private ValueTask<RaftCommandResult> ForwardCommandImpl(RaftCommand command)
        {
            var id = command.Id;
            Debug.Assert(LeaderUid != null && _channel != null);
            var op = _operationPool.Get();
            Debug.Assert(op.TryOwnAndReset());
            lock (_syncRoot)
            {
                _forwardedCommands.Add(id, op);

            }

            var length = command.GetLength(_config.ReaderWriter);


            if (length < 128)
            {
                Span<byte> buffer = stackalloc byte[length];
                command.TryWrite(buffer, out _, _config.ReaderWriter);

                ReadOnlySpan<byte> span = buffer;
                _channel.ForwardOperationToPrimary(ShardUid, LeaderUid.Value, ref span);
            }
            else
            {
                using var owner = _config.MemoryPool.Rent(length);
                command.TryWrite(owner.Memory.Span, out length, _config.ReaderWriter);
                ReadOnlySpan<byte> span = owner.Memory.Span.Slice(0, length);
                _channel.ForwardOperationToPrimary(ShardUid, LeaderUid.Value, ref span);
            }

            async ValueTask<RaftCommandResult> ProcessTaskCompletion(Guid id, AsyncOperation<RaftCommandResult> asyncOperation)
            {
                try
                {
                    return await asyncOperation.ValueTaskOfT;
                }
                finally
                {
                    _forwardedCommands.Remove(id);
                    _operationPool.Return(asyncOperation);
                }
            }
            ;

            return ProcessTaskCompletion(id, op);

        }


        private bool AppendEntriesToReplica(bool force)
        {
            bool mustWait = false;
            foreach (var server in _backend.CurrentShardsConfiguration.All)
            {
                if (server.Uid != ShardUid && IsShardConnected(server.Uid))
                {
                    var id = server.Uid;
                    if (!_shardInstances.TryGetValue(id, out var state))
                    {
                        state = new ShardReplicaSynchronisationState(id, _backend.LastLogEntry);
                        _shardInstances[id] = state;
                    }
                    mustWait = true;

                    _ = AppendEntriesAsync(state);

                }
            }
            return mustWait;

        }


        public bool IsShardConnected(Guid shardUid)
        {
            return _channel?.IsShardConnected(shardUid) ?? false;
        }

        public bool IsShardVoting(Guid shardUid)
        {
            if (ShardUid == shardUid && (_channel == null || _backend.CurrentShardsConfiguration.New == null))
            {
                return true;
            }
            else
            {
                return IsShardConnected(shardUid) && _backend.CurrentShardsConfiguration.IsVoting(shardUid);
            }
        }


        private async Task AppendEntriesAsync(ShardReplicaSynchronisationState state)
        {
            if (_channel == null)
            {
                return;
            }
            lock (state.SyncRoot)
            {
                if (state.AppendInProgress)
                {
                    return;
                }
                state.AppendInProgress = true;
            }
            while (true)
            {


                lock (state.SyncRoot)
                {
                    if (!IsLeader || !IsShardVoting(state.ShardUid))
                    {
                        return;
                    }



                }

                var targetId = state.ShardUid;
                var firstEntryId = state.NextLogEntryIdToSend;


                var lastEntryId = _backend.LastLogEntry;




                var getEntriesResult = await _backend.GetEntries(firstEntryId, lastEntryId);
                var entries = getEntriesResult.Entries;

                ulong prevLogEntryId = getEntriesResult.PrevLogEntryId;
                ulong prevLogEntryTerm = getEntriesResult.PrevLogEntryTerm;
                firstEntryId = getEntriesResult.FirstEntryId;
                lastEntryId = getEntriesResult.LastEntryId;


                ShardsReplicationLogging.LogSendingAppendCommand(_logger, ShardUid, targetId, _backend.CurrentTerm, firstEntryId, lastEntryId, _commitIndex);

                var result = await _channel.AppendEntriesAsync(
                    this.ShardUid,
                    targetId,
                    _backend.CurrentTerm,
                    entries,
                    _backend.LastLogEntry,
                    prevLogEntryId,
                    prevLogEntryTerm,
                    _commitIndex);

                ShardsReplicationLogging.LogReceivedAppendEntriesResult(_logger, result.Success, targetId, result.Term, result.LastLogEntryId, result.LastAppliedLogEntryId);
                lock (state.SyncRoot)
                {
                    if (state.LastAppliedLogEntryId < result.LastAppliedLogEntryId)
                    {
                        state.LastAppliedLogEntryId = result.LastAppliedLogEntryId;
                        ProcessCompletedOperations();
                    }
                    if (result.Term > _backend.CurrentTerm && LeaderUid != result.LeaderId && result.LeaderId != null)
                    {

                        this.LeaderUid = result.LeaderId;
                        UpdateTerm(result.Term);

                        state.AppendInProgress = false;
                        return;
                    }
                    else if (!result.Success)
                    {
                        state.LastAppendSuccess = false;
                        var candidate = state.NextLogEntryIdToSend - 1;
                        state.NextLogEntryIdToSend = candidate < (result.LastLogEntryId + 1) ? candidate : (result.LastLogEntryId + 1);


                    }
                    else
                    {
                        if (result.LastLogEntryId > state.LastKnownReplicatedLogEntry)
                        {
                            _logger.Log(LogLevel.Debug, "Updated {shard} last known log entry from {lastLogEntryId} to {newLastLogEntryId}", state.ShardUid, state.LastKnownReplicatedLogEntry, result.LastLogEntryId);
                            state.LastKnownReplicatedLogEntry = result.LastLogEntryId;
                        }

                        if (!state.LastAppendSuccess)
                        {
                            state.LastAppendSuccess = true;

                        }
                        state.NextLogEntryIdToSend = result.LastLogEntryId + 1;
                        TryCommit();
                        if (state.LastKnownReplicatedLogEntry == _backend.LastLogEntry && state.LastAppliedLogEntryId == _backend.LastAppliedLogEntry)
                        {
                            state.AppendInProgress = false;
                            return;
                        }

                    }
                }
            }




        }

        private Task AppendEntries(ShardReplicaSynchronisationState state, bool force)
        {
            throw new NotImplementedException();

            //if (state.AppendInProgress && !force)
            //{
            //    return;
            //}
            //lock (state.SyncRoot)
            //{
            //    if (IsLeader)
            //    {
            //        if (state.AppendInProgress && !force)
            //        {
            //            return;
            //        }


            //        state.AppendInProgress = true;
            //        state.LastAppendEntriesSentOn = DateTime.UtcNow;

            //    }
            //}

            //if (IsLeader)
            //{
            //    var targetId = state.ShardUid;
            //    var firstEntryId = state.NextLogEntryIdToSend;


            //    var lastEntryId = _backend.LastLogEntry;



            //    ulong prevLogEntryId;
            //    ulong prevLogEntryTerm;
            //    var entries = await _backend.GetEntries(ref firstEntryId, ref lastEntryId, out prevLogEntryId, out prevLogEntryTerm);




            //    ShardsReplicationLogging.LogSendingAppendCommand(_logger, ShardUid, targetId, _backend.CurrentTerm, firstEntryId, lastEntryId, _commitIndex);
            //    _channel?.AppendEntries(
            //        this.ShardUid,
            //        targetId,
            //        _backend.CurrentTerm,
            //        entries,
            //        _backend.LastLogEntry,
            //        prevLogEntryId,
            //        prevLogEntryTerm,
            //        _commitIndex);

            //    lock (state.SyncRoot)
            //    {

            //        if (state.LastAppendSuccess)
            //        {
            //            state.NextLogEntryIdToSend = lastEntryId + 1;
            //        }
            //    }
            //}




        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private AppendEntriesResult CreateAppendEntriesResult(Guid dest, bool success)
        {
            ShardsReplicationLogging.LogSendingAppendCommandResponse(_logger, ShardUid, dest, success, LeaderUid, _backend.CurrentTerm, _backend.LastLogEntry, _backend.LastAppliedLogEntry);
            return new AppendEntriesResult
            {
                LeaderId = LeaderUid,
                Success = success,
                Term = _backend.CurrentTerm,
                LastLogEntryId = _backend.LastLogEntry,
                LastAppliedLogEntryId = _backend.LastAppliedLogEntry,
            };
        }
        AppendEntriesResult IReplicatedStorageMessageHandler.OnAppendEntries(ulong term, Guid leaderId, IEnumerable<LogEntry> entries, ulong lastLeaderEntryId, ulong prevLogIndex, ulong prevLogTerm, ulong leaderCommit)
        {

            /*  Receiver implementation:
                1. Reply false if term < currentTerm (§5.1)
                2. Reply false if log doesn't contain an entry at prevLogIndex
                whose term matches prevLogTerm (§5.3)
                3. If an existing entry conflicts with a new one (same index
                but different terms), delete the existing entry and all that
                follow it (§5.3)
                4. Append any new entries not already in the log
                5. If leaderCommit > commitIndex, set commitIndex =
                min(leaderCommit, index of last new entry)
             */
            if (term < _backend.CurrentTerm)
            {
                return CreateAppendEntriesResult(leaderId, false);

            }
            else if (term > _backend.CurrentTerm || LeaderUid == null)
            {

                UpdateTerm(term);

            }
            if (this.LeaderUid != leaderId)
            {
                this.LeaderUid = leaderId;
            }

            if (prevLogIndex > 0)
            {
                if (!_backend.TryGetEntryTerm(prevLogIndex, out var entryTerm) || entryTerm != prevLogTerm)
                {
                    return CreateAppendEntriesResult(leaderId, false);
                }
            }



            if (!_backend.TryAppendEntries(entries, out var _))
            {
                return CreateAppendEntriesResult(leaderId, false);
            }

            //If this replica contains more entries than the elected leader, we know these entries will be overwritten in the future and the associated commands will fail. Truncate the log now to fail any pending command immediately.
            if (_backend.LastLogEntry > lastLeaderEntryId)
            {
                _backend.TryTruncateEntriesAfter(lastLeaderEntryId);
            }

            _commitIndex = leaderCommit;

            if (_commitIndex > _backend.LastAppliedLogEntry)
            {
                _backend.ApplyEntries(_commitIndex);
            }

            return CreateAppendEntriesResult(leaderId, true);

        }

        void IReplicatedStorageMessageHandler.OnAppendEntriesResult(Guid origin, Guid? leaderId, ulong term, ulong firstLogEntryInRequest, ulong lastLogEntry, ulong lastReplicatedIndex, bool success)
        {



            bool commitHappened = false;


            if (_shardInstances.TryGetValue(origin, out var shardInstance))
            {
                lock (shardInstance.SyncRoot)
                {
                    shardInstance.AppendInProgress = false;
                    if (term > _backend.CurrentTerm && LeaderUid != leaderId && leaderId != null)
                    {
                        this.LeaderUid = leaderId;
                        UpdateTerm(term);

                        return;
                    }
                    else if (!success)
                    {
                        if (IsShardConnected(origin))
                        {

                            shardInstance.LastAppendSuccess = false;

                            var candidate = firstLogEntryInRequest - 1;
                            if (candidate < shardInstance.NextLogEntryIdToSend)
                            {

                                shardInstance.NextLogEntryIdToSend = candidate < (lastLogEntry + 1) ? candidate : (lastLogEntry + 1);

                            }
                            //We start the next synchronization attempt and don't wait for completion.
                            _ = AppendEntries(shardInstance, true);
                        }
                        else
                        {
                            _shardInstances.Remove(origin);
                            if (IsLeader && TryCommit())
                            {
                                commitHappened = true;
                            }

                        }

                    }
                    else
                    {

                        if (lastLogEntry > shardInstance.LastKnownReplicatedLogEntry)
                        {
                            shardInstance.LastKnownReplicatedLogEntry = lastLogEntry;
                        }

                        if (!shardInstance.LastAppendSuccess)
                        {
                            shardInstance.LastAppendSuccess = true;
                            shardInstance.NextLogEntryIdToSend = lastLogEntry + 1;
                        }


                        if (IsLeader && TryCommit())
                        {
                            commitHappened = true;
                        }
                        else if (IsLeader && shardInstance.LastKnownReplicatedLogEntry != this._backend.LastLogEntry)
                        {
                            //We start the next synchronization attempt and don't wait for completion.
                            _ = AppendEntries(shardInstance, true);
                        }

                    }
                }

            }

            if (commitHappened)
            {
                //We are certain that AppendEntries wasn't called by the function.
                AppendEntriesToReplica(true);
            }
        }

        private bool TryCommit()
        {
            if (_backend.LastLogEntry == _backend.LastAppliedLogEntry)
            {
                return false;
            }

            lock (_syncRoot)
            {
                try
                {
                    _logger.Log(LogLevel.Trace, "start commit {committed}/{lastEntryId}", _backend.LastAppliedLogEntry,_backend.LastLogEntry);
                    var shardInstancesCount = _shardInstances.Count + 1;
                    var totalSynchronized = 0;
                    var total = 0;
                    for (var commitCandidate = _backend.LastLogEntry; commitCandidate > _backend.LastAppliedLogEntry; commitCandidate--)
                    {
                        totalSynchronized = 0;
                        total = 0;
                        foreach (var shardInstance in _shardInstances.Values)
                        {
                            if (IsShardVoting(shardInstance.ShardUid))
                            {
                                total++;
                                if (shardInstance.LastKnownReplicatedLogEntry >= commitCandidate)
                                {
                                    totalSynchronized++;
                                }
                            }
                        }

                        if (IsShardVoting(ShardUid))
                        {
                            total++;
                            totalSynchronized++;
                        }

                        _logger.LogDebug("{commitCandidate} : {totalSynchronized}/{total}", commitCandidate, totalSynchronized, total);
                        /*
                         5.3
                         * To eliminate problems like the one in Figure 8, Raft
                        never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
                        term are committed by counting replicas; once an entry
                        from the current term has been committed in this way,
                        then all prior entries are committed indirectly because
                        of the Log Matching Property. There are some situations
                        where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach
                        for simplicity
                         */
                        if (totalSynchronized * 2 > total && _backend.TryGetEntryTerm(commitCandidate, out var term) && term == _backend.CurrentTerm)
                        {
                            this._commitIndex = commitCandidate;

                            _backend.ApplyEntries(this._commitIndex);
                            ProcessCompletedOperations();
                            ShardsReplicationLogging.LogSuccessfulLocalCommitAttempt(_logger, _commitIndex, totalSynchronized, total);
                            return true;
                        }


                    }

                    ShardsReplicationLogging.LogFailedLocalCommitAttempt(_logger, _commitIndex,_backend.LastAppliedLogEntry,_backend.LastLogEntry, totalSynchronized, total);


                    return false;
                }
                catch (Exception ex)
                {
                    _logger.Log(LogLevel.Error, "Failed to apply entries {ex}", ex);
                    return false;
                }

            }
        }


        bool IReplicatedStorageMessageHandler.TryProcessForwardOperation(Guid originUid, ReadOnlySequence<byte> input, out int bytesRead)
        {
            throw new NotImplementedException();
        }

        void IReplicatedStorageMessageHandler.ProcessForwardOperationResponse(Guid originUid, ReadOnlySequence<byte> responseInput, out int bytesRead)
        {
            if (RaftCommandResult.TryRead(responseInput, out bytesRead, out var result) && _forwardedCommands.TryGetValue(result.OperationId, out var asyncOperation))
            {
                asyncOperation.TrySetResult(result);

            }
        }

        private bool TrySetAsLeader()
        {

            if (_channel != null && !_backend.CurrentShardsConfiguration.IsVoting(ShardUid))
            {
                return false;
            }


            LeaderUid = ShardUid;

            //Send an append entry command on election to trigger lease.

            return true;
        }

        public void StepDownLeadership()
        {
            if (!IsLeader)
            {
                return;
            }
            else
            {
                LeaderUid = null;
            }
        }

        private void UpdateTerm(ulong newTerm)
        {
            lock (_syncRoot)
            {
                _backend.UpdateTerm(newTerm);
                _votedFor = null;

            }
        }

        private AsyncOperation<bool>? _electAsLeaderOperation;

        public ValueTask<bool> ElectAsLeaderAsync()
        {
            if (LeaderUid == ShardUid)
            {
                return ValueTask.FromResult(true);
            }
            if (_channel == null)
            {
                if (TrySetAsLeader())
                {
                    var result = ExecuteNoOp();

                    return ValueTask.FromResult(result.Success);
                }
                else
                {
                    return ValueTask.FromResult(false);
                }

            }

            if (!IsShardVoting(ShardUid))
            {
                return ValueTask.FromResult(false);
            }


            if (_electAsLeaderOperation != null)
            {
                return _electAsLeaderOperation.ValueTaskOfT;
            }

            var operation = new AsyncOperation<bool>(true);
            lock (_syncRoot)
            {
                _electAsLeaderOperation = operation;

                LeaderUid = null;
                UpdateTerm(_backend.CurrentTerm + 1);
                _votedFor = ShardUid;
            }


            async Task ProcessElection()
            {
                var tasks = new List<Task<RequestVoteResult>>();
                foreach (var shard in _backend.CurrentShardsConfiguration.All)
                {
                    if (shard.Uid != ShardUid && IsShardConnected(shard.Uid))
                    {
                        ShardsReplicationLogging.SendingRequestVote(_logger, shard.Uid, _backend.CurrentTerm, _backend.LastLogEntry, _backend.LastLogEntryTerm);
                        tasks.Add(_channel.RequestVoteAsync(ShardUid, shard.Uid, _backend.CurrentTerm, _backend.LastLogEntry, _backend.LastLogEntryTerm));
                    }

                }


                var results = await Task.WhenAll(tasks);
                bool elected = false;
                var total = 1;
                var votes = 1;
                lock (_syncRoot)
                {
                    if (LeaderUid == null)
                    {

                        foreach (var result in results)
                        {
                            if (result.Term == _backend.CurrentTerm && result.RequestSuccess)
                            {
                                if (result.VoteGranted)
                                {
                                    votes++;
                                }
                                total++;
                            }
                        }

                        var majority = ((total) / 2) + 1;
                        if (votes >= majority)
                        {

                            elected = TrySetAsLeader();

                        }
                        else
                        {
                            ShardsReplicationLogging.ElectionCompleted(_logger, LeaderUid ?? Guid.Empty, false, votes, total);
                            operation.TrySetResult(false);
                        }

                    }


                    _electAsLeaderOperation = null;
                }

                if (elected)
                {
                    ShardsReplicationLogging.ElectionCompleted(_logger, LeaderUid ?? Guid.Empty, true, votes, total);
                    var result = ExecuteNoOp();
                    var success = await WaitCommitted(result);
                    operation.TrySetResult(success);
                }
            }

            _ = ProcessElection();





            return operation.ValueTaskOfT;

        }

        private RequestVoteResult CreateRequestVoteResult(Guid candidateId, bool voteGranted)
        {
            ShardsReplicationLogging.SendingRequestVoteResult(_logger, ShardUid, candidateId, voteGranted, _votedFor, _backend.CurrentTerm, _backend.LastLogEntry, _backend.LastLogEntryTerm);
            return new RequestVoteResult { Term = _backend.CurrentTerm, VoteGranted = voteGranted };
        }
        RequestVoteResult IReplicatedStorageMessageHandler.OnRequestVote(ulong term, Guid candidateId, ulong lastLogIndex, ulong lastLogTerm)
        {
            lock (_syncRoot)
            {
                if (term > _backend.CurrentTerm)
                {
                    StepDownLeadership();
                    UpdateTerm(term);


                }
                if (term < _backend.CurrentTerm)
                {
                    return CreateRequestVoteResult(candidateId, false);
                }

                else if (lastLogTerm < _backend.LastLogEntryTerm)
                {
                    return CreateRequestVoteResult(candidateId, false);

                }
                else if (lastLogTerm == _backend.LastLogEntryTerm && lastLogIndex < _backend.LastLogEntry)
                {
                    return CreateRequestVoteResult(candidateId, false);

                }
                else if (_votedFor != null)
                {
                    return CreateRequestVoteResult(candidateId, _votedFor == candidateId);

                }
                else
                {
                    if (IsLeader)
                    {
                        StepDownLeadership();
                    }
                    _votedFor = candidateId;
                    return CreateRequestVoteResult(candidateId, true);
                }


            }
        }
    }
}
