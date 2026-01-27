using System.Buffers;

namespace Stormancer.Raft
{


    public struct AppendEntriesResult
    {

        public ulong Term { get; set; }

        public bool Success { get; set; }

        public Guid? LeaderId { get; set; }

        public ulong LastLogEntryId { get; set; }
        public ulong LastAppliedLogEntryId { get; set; }
    }
    public struct RequestVoteResult
    {
        public ulong Term { get; set; }
        public bool VoteGranted { get; set; }
        public bool RequestSuccess { get; set; }
    }

    public class InstallSnapshotArguments
    {
        public ulong Term { get; set; }
        public Guid LeaderId { get; set; }
        public ulong LastIncludedIndex { get; set; }
        public ulong LastIncludedTerm { get; set; }
    }

    public class InstallSnapshotResult
    {
        public ulong Term { get; set; }
    }


    public interface IReplicatedStorageMessageChannel
    {

        
        Task<AppendEntriesResult> AppendEntriesAsync(Guid origin, Guid destination, ulong term, IEnumerable<LogEntry> entries, ulong lastLeaderEntryId, ulong prevLogIndex, ulong prevLogTerm, ulong leaderCommit);

        Task<RequestVoteResult> RequestVoteAsync(Guid candidateId, Guid destination, ulong term, ulong lastLogIndex, ulong lastLogTerm);

        bool IsShardConnected(Guid shardUid);

        void ForwardOperationToPrimary(Guid origin, Guid leaderUid,ref ReadOnlySpan<byte> operation);
      
        void SendForwardOperationResult(Guid origin, Guid destination,ref ReadOnlySpan<byte> result);

   

    }

    public interface IReplicatedStorageMessageHandler
    {

        AppendEntriesResult OnAppendEntries(ulong term, Guid leaderId, IEnumerable<LogEntry> entries, ulong lastLeaderEntryId, ulong prevLogIndex, ulong prevLogTerm, ulong leaderCommit);

        void OnAppendEntriesResult(Guid origin, Guid? leaderId, ulong term, ulong firstLogEntry, ulong lastLogEntry, ulong lastReplicatedIndex, bool success);

        RequestVoteResult OnRequestVote(ulong term, Guid candidateId, ulong lastLogIndex, ulong lastLogTerm);

     

        bool TryProcessForwardOperation(Guid originUid, ReadOnlySequence<byte> input, out int bytesRead);

        void ProcessForwardOperationResponse(Guid originUid, ReadOnlySequence<byte> responseInput, out int bytesRead);
    }





}
