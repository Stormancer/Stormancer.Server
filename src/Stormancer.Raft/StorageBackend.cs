using Stormancer.Raft.WAL;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    

    public interface IStorageShardBackend
    {
        ulong LastAppliedLogEntry { get; }

        ulong LastLogEntry { get; }
        ulong LastLogEntryTerm { get; }
        ulong CurrentTerm { get; }




        RaftCommandResult TryAppendCommand(RaftCommand command);
        
        bool TryAppendEntries(IEnumerable<LogEntry> entries,[NotNullWhen(false)] out Error? error);

        void ApplyEntries(ulong index);

        ValueTask<GetEntriesResult> GetEntries( ulong firstEntryId,  ulong lastEntryId);

        bool TryTruncateEntriesAfter(ulong logEntryId);

        bool TryGetEntryTerm(ulong prevLogId, out ulong entryTerm);

        void UpdateTerm(ulong term);



        ShardsConfigurationRecord CurrentShardsConfiguration { get; }
    }
}
