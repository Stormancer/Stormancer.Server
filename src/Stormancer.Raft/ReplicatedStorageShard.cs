using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Stormancer.Raft;
using Stormancer.Threading;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Stormancer.Raft
{

    internal class NoOpSystemRecord : IRecord<NoOpSystemRecord>
    {
        private NoOpSystemRecord() { }
        public static NoOpSystemRecord Instance { get; } = new NoOpSystemRecord();

        public static bool TryRead(ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out NoOpSystemRecord? record, out int length)
        {
            throw new NotImplementedException();
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out NoOpSystemRecord? record, out int length)
        {
            throw new NotImplementedException();
        }

        public int GetLength()
        {
            throw new NotImplementedException();
        }

        public bool TryWrite(ref Span<byte> buffer, out int length)
        {
            throw new NotImplementedException();
        }
    }
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
    }
    public interface ICommand<T> where T : ICommand<T>
    {
        Guid Id { get; }
        TSystemCommandContent? As<TSystemCommandContent>() where TSystemCommandContent : class, IRecord<TSystemCommandContent>;
        static abstract T Create(IRecord systemRecord);
    }

    public interface ICommandResult<T> where T : ICommandResult<T>
    {
        Guid OperationId { get; }

        [MemberNotNullWhen(false, "Error")]
        bool Success { get; }

        Error? Error { get; }

        int GetLength();
        void Write(Span<byte> span);
        static abstract bool TryRead(ReadOnlySequence<byte> buffer, out int bytesRead, [NotNullWhen(true)] out T? result);

        static abstract T CreateFailed(Guid operationId, Error error);
    }










    public class ReplicatedStorageShard<TCommandResult> : IReplicatedStorageMessageHandler
    where TCommandResult : ICommandResult<TCommandResult>

    {
        private class ShardReplicaSynchronisationState(Guid shardUid, ulong nextLogEntryIdToSend)
        {
            public Guid ShardUid { get; set; } = shardUid;

            public ulong NextLogEntryIdToSend { get; set; } = nextLogEntryIdToSend;

            public ulong LastKnownReplicatedLogEntry { get; set; } = 0;

            public DateTime LastAppendEntriesSentOn { get; set; } = DateTime.MinValue;
            public bool LastAppendSuccess { get; internal set; } = true;
            public bool AppendInProgress { get; internal set; } = false;

            public object SyncRoot = new object();
        }

        private readonly Dictionary<Guid, ShardReplicaSynchronisationState> _shardInstances = new Dictionary<Guid, ShardReplicaSynchronisationState>();


        private readonly object _syncRoot = new object();

        private readonly ILogger _logger;
        private ulong _commitIndex = 0;
        private Guid? _votedFor = null;

        private readonly IReplicatedStorageMessageChannel? _channel;
        private readonly IStorageShardBackend<TCommandResult> _backend;

        private readonly ReplicatedStorageShardConfiguration _config;

        public Guid ShardUid { get; }

        public Guid? LeaderUid { get; private set; }


        public bool IsLeader => LeaderUid == ShardUid;

        public ReplicatedStorageShard(Guid shardUid,
            ReplicatedStorageShardConfiguration config,
            ILoggerFactory logger,
            IReplicatedStorageMessageChannel? channel,
            IStorageShardBackend<TCommandResult> backend
            )
        {
            ArgumentNullException.ThrowIfNull(config, nameof(config));
            _config = config;
            ShardUid = shardUid;

            _channel = channel;
            _backend = backend;
            _logger = logger.CreateLogger(ShardUid.ToString());

        }



        public ValueTask<TCommandResult> ExecuteCommand(RaftCommand command)
        {
            ShardsReplicationLogging.LogStartProcessingCommand(_logger, command.Id);

            if (IsLeader)
            {

                if (_backend.TryAppendCommand(command, out var entry, out var error))
                {
                    var task = _backend.WaitCommittedAsync(entry.Id);
                    if (!AppendEntriesToReplica(false))
                    {
                        //no replica, we should commit immediately.
                        TryCommit();
                    }
                    return task;

                }
                else
                {
                    return ValueTask.FromResult(TCommandResult.CreateFailed(command.Id, error));
                }
            }
            else
            {
                return ForwardCommand(command);
            }


        }

        private ValueTask<TCommandResult> ExecuteNoOp()
        {


            if (IsLeader)
            {

                if (_backend.TryAppendCommand(TCommand.Create(NoOpSystemRecord.Instance), out var entry, out var error))
                {
                    var task = _backend.WaitCommittedAsync(entry.Id);
                    if (!AppendEntriesToReplica(false))
                    {
                        //no replica, we should commit immediately.
                        TryCommit();
                    }
                    return task;

                }
                else
                {
                    return ValueTask.FromResult(TCommandResult.CreateFailed(Guid.NewGuid(), error));
                }
            }
            else
            {
                return ValueTask.FromResult(TCommandResult.CreateFailed(Guid.NewGuid(), new Error(ShardErrors.NotLeader, null)));
            }



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


            await UpdateClusterConfiguration(new ShardsConfigurationRecord(_backend.CurrentShardsConfiguration.New, new HashSet<Server>(newConfiguration)));

            await UpdateClusterConfiguration(new ShardsConfigurationRecord(null, new HashSet<Server>(newConfiguration)));
            if (!IsShardVoting(ShardUid))
            {
                StepDownLeadership();
            }
        }

        private async ValueTask UpdateClusterConfiguration(ShardsConfigurationRecord record)
        {
            ValueTask<TCommandResult> task;
            lock (_syncRoot)
            {

                if (_backend.TryAppendCommand(TCommand.Create(record), out var entry, out var error))
                {
                    task = _backend.WaitCommittedAsync(entry.Id);
                    if (!AppendEntriesToReplica(false))
                    {
                        //no replica, we should commit immediately.
                        TryCommit();
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Failed to apply the configuration change because : {error}");
                }
            }

            await task;
        }

        private Dictionary<Guid, AsyncOperation<TCommandResult>> _forwardedCommands = new Dictionary<Guid, AsyncOperation<TCommandResult>>();

        private ObjectPool<AsyncOperation<TCommandResult>> _operationPool = new DefaultObjectPool<AsyncOperation<TCommandResult>>(new AsyncOperationPoolPolicy<TCommandResult>());
        private ValueTask<TCommandResult> ForwardCommand(RaftCommand command)
        {
            if (IsLeader)
            {
                return ExecuteCommand(command);
            }
            else if (LeaderUid == null)
            {
                return ValueTask.FromResult(TCommandResult.CreateFailed(command.Id, new Error(RaftErrors.NotAvailable, null)));
            }
            else if (_channel != null)
            {
                return ForwardCommandImpl(command);
            }
            else
            {
                return ValueTask.FromResult(TCommandResult.CreateFailed(command.Id, new Error(RaftErrors.NotAvailable, "communication channel unavailable.")));
            }
        }

        private ValueTask<TCommandResult> ForwardCommandImpl(RaftCommand command)
        {
            var id = command.Id;
            Debug.Assert(LeaderUid != null && _channel != null);
            var op = _operationPool.Get();
            Debug.Assert(op.TryOwnAndReset());
            lock (_syncRoot)
            {
                _forwardedCommands.Add(id, op);

            }
           
            var length = _config.ReaderWriter.GetContentLength(command.Record);


            if (length < 128)
            {
                Span<byte> buffer = stackalloc byte[length];
                command.Write(buffer);
                ReadOnlySpan<byte> span = buffer;
                _channel.ForwardOperationToPrimary(ShardUid, LeaderUid.Value, ref span);
            }
            else
            {
                using var owner = _config.MemoryPool.Rent(length);
                command.Write(owner.Memory.Span);
                ReadOnlySpan<byte> span = owner.Memory.Span.Slice(0, length);
                _channel.ForwardOperationToPrimary(ShardUid, LeaderUid.Value, ref span);
            }

            async ValueTask<TCommandResult> ProcessTaskCompletion(Guid id, AsyncOperation<TCommandResult> asyncOperation)
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
            };

            return ProcessTaskCompletion(id,op);

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
            return IsShardConnected(shardUid) && _backend.CurrentShardsConfiguration.IsVoting(shardUid);
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


                lock (state.SyncRoot)
                {
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
                            state.LastKnownReplicatedLogEntry = result.LastLogEntryId;
                        }

                        if (!state.LastAppendSuccess)
                        {
                            state.LastAppendSuccess = true;

                        }
                        state.NextLogEntryIdToSend = result.LastLogEntryId + 1;

                        if (!TryCommit() && state.LastKnownReplicatedLogEntry == _backend.LastLogEntry)
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
            ShardsReplicationLogging.LogSendingAppendCommandResponse(_logger, dest, success, LeaderUid, _backend.CurrentTerm, _backend.LastLogEntry, _backend.LastAppliedLogEntry);
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



            if (!_backend.TryAppendEntries(entries))
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
            lock (_syncRoot)
            {


                var shardInstancesCount = _shardInstances.Count + 1;
                for (var commitCandidate = _backend.LastLogEntry; commitCandidate > _backend.LastAppliedLogEntry; commitCandidate--)
                {
                    var totalSynchronized = 0;
                    var total = 0;
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
                    /*
                     5.3
                     * To eliminate problems like the one in Figure 8, Raft
                    never commits log entries from previous terms by counting replicas. Only log entries from the leader’s current
                    term are committed by counting replicas; once an entry
                    from the current term has been committed in this way,
                    then all prior entries are committed indirectly because
                    of the Log Matching Property. There are some situations
                    where a leader could safely conclude that an older log entry is committed (for example, if that entry is stored on every server), but Raft takes a more conservative approach
                    for simplicity
                     */
                    if (totalSynchronized * 2 > total && _backend.TryGetEntryTerm(commitCandidate, out var term) && term == _backend.CurrentTerm)
                    {
                        this._commitIndex = commitCandidate;

                        _backend.ApplyEntries(this._commitIndex);
                        return true;
                    }


                }
                return false;
            }
        }


        public bool TryProcessForwardOperation(Guid originUid, ReadOnlySequence<byte> input, out int bytesRead)
        {
            throw new NotImplementedException();
        }

        public void ProcessForwardOperationResponse(Guid originUid, ReadOnlySequence<byte> responseInput, out int bytesRead)
        {
            if (TCommandResult.TryRead(responseInput, out bytesRead, out var result) && _forwardedCommands.TryGetValue(result.OperationId, out var asyncOperation))
            {
                asyncOperation.TrySetResult(result);

            }
        }

        private bool TrySetAsLeader()
        {
            
            if (_channel !=null &&  !_backend.CurrentShardsConfiguration.IsVoting(ShardUid))
            {
                return false;
            }


            LeaderUid = ShardUid;

            //Send an append entry command on election to trigger lease.
            _ = ExecuteNoOp();
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

                return ValueTask.FromResult(TrySetAsLeader());
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
                lock (_syncRoot)
                {
                    if (LeaderUid == null)
                    {
                        var total = 1;
                        var votes = 1;
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

                            operation.TrySetResult(TrySetAsLeader());
                        }
                        else
                        {
                            operation.TrySetResult(false);
                        }

                    }
                    _electAsLeaderOperation = null;
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
