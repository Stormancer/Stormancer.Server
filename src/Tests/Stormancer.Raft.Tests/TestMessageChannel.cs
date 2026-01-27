using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.Tests
{
    internal class TestMessageChannel : IReplicatedStorageMessageChannel
    {
        MemoryPool<byte> _memoryPool = MemoryPool<byte>.Shared;
        private class ShardInstance
        {
            public ShardInstance(IReplicatedStorageMessageHandler handler)
            {
                Handler = handler;
            }

            public IReplicatedStorageMessageHandler Handler { get; }
        }

        private readonly Dictionary<Guid, ShardInstance> _shards = new Dictionary<Guid, ShardInstance>();
        private readonly Func<int> _latencyGenerator;

        public TestMessageChannel(Func<int> latencyGenerator)
        {
            _latencyGenerator = latencyGenerator;
        }

        public void AddShard(Guid id, IReplicatedStorageMessageHandler shard)
        {
            _shards.Add(id, new ShardInstance(shard));
        }

        public void RemoveShard(Guid id)
        {
            _shards.Remove(id);
        }


        public async Task<AppendEntriesResult> AppendEntriesAsync(Guid origin, Guid destination, ulong term, IEnumerable<LogEntry> entries, ulong lastLeaderEntryId, ulong prevLogIndex, ulong prevLogTerm, ulong leaderCommit)
        {
            if (_shards.TryGetValue(origin, out var shard))
            {
                var latency = _latencyGenerator();
                if (latency > 0)
                {
                    await Task.Delay(latency);
                }
                var result = shard.Handler.OnAppendEntries(term, origin, entries, lastLeaderEntryId, prevLogIndex, prevLogTerm, leaderCommit);
                latency = _latencyGenerator();
                if (latency > 0)
                {
                    await Task.Delay(latency);
                }
                return result;
            }
            else
            {
                return new AppendEntriesResult { Success = false, };
            }
        }

        public bool IsShardConnected(Guid shardUid)
        {
            return _shards.ContainsKey(shardUid);
        }

        public void ForwardOperationToPrimary(Guid origin, Guid leaderUid, ref ReadOnlySpan<byte> operation)
        {
            if (_shards.TryGetValue(leaderUid, out var shard))
            {

                using var owner = _memoryPool.Rent(operation.Length);
                operation.CopyTo(owner.Memory.Span);
                shard.Handler.TryProcessForwardOperation(origin, new ReadOnlySequence<byte>(owner.Memory.Slice(0, operation.Length)), out _);

            }
        }

        public void SendForwardOperationResult(Guid origin, Guid destination, ref ReadOnlySpan<byte> result)
        {
            if (_shards.TryGetValue(destination, out var shard))
            {
                using var owner = _memoryPool.Rent(result.Length);
                result.CopyTo(owner.Memory.Span);
                shard.Handler.ProcessForwardOperationResponse(origin, new ReadOnlySequence<byte>(owner.Memory.Slice(0, result.Length)), out _);
            }
        }

        public async Task<RequestVoteResult> RequestVoteAsync(Guid candidateId, Guid destination, ulong term, ulong lastLogIndex, ulong lastLogTerm)
        {
            if (_shards.TryGetValue(destination, out var state))
            {
                await Task.Delay(_latencyGenerator());
                var result = state.Handler.OnRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
                await Task.Delay(_latencyGenerator());
                result.RequestSuccess = true;
                return result;
            }
            else
            {
                return new RequestVoteResult { Term = term, VoteGranted = false, RequestSuccess = false };
            }

        }

    }
}
