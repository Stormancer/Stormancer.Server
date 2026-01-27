using Stormancer.Raft.WAL;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    

    public interface IStorageShardBackend<TCommandResult>
       where TCommandResult : ICommandResult<TCommandResult>
    {
        ulong LastAppliedLogEntry { get; }

        ulong LastLogEntry { get; }
        ulong LastLogEntryTerm { get; }
        ulong CurrentTerm { get; }




        bool TryAppendCommand(RaftCommand command, [NotNullWhen(true)] out LogEntry? entry, [NotNullWhen(false)] out Error? error);
        
        bool TryAppendEntries(IEnumerable<LogEntry> entries);

        void ApplyEntries(ulong index);

        ValueTask<GetEntriesResult> GetEntries( ulong firstEntryId,  ulong lastEntryId);

        bool TryTruncateEntriesAfter(ulong logEntryId);

        bool TryGetEntryTerm(ulong prevLogId, out ulong entryTerm);
        ValueTask<TCommandResult> WaitCommittedAsync(ulong entryId);
        void UpdateTerm(ulong term);



        ShardsConfigurationRecord CurrentShardsConfiguration { get; }
    }
}
