using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.ObjectPool;
using Stormancer.Threading;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.WAL
{
    public class RaftMetadata : IRecord<RaftMetadata>
    {
        public static bool TryRead(ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out RaftMetadata? record, out int length)
        {
            length = 16;
            if (buffer.Length < 16)
            {
                record = null;

                return false;
            }
            Span<byte> b = stackalloc byte[8];

            var reader = new SequenceReader<byte>(buffer);
            reader.TryCopyTo(b);
            reader.Advance(8);
            var currentTerm = BinaryPrimitives.ReadUInt64BigEndian(b);

            reader.TryCopyTo(b);
            var lastApplied = BinaryPrimitives.ReadUInt64BigEndian(b);
            record = new RaftMetadata { CurrentTerm = currentTerm, LastAppliedLogEntry = lastApplied };
            return true;
        }

        public static bool TryRead(ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out RaftMetadata? record, out int length)
        {
            length = 16;
            if (buffer.Length < 16)
            {
                record = null;

                return false;
            }
            Span<byte> b = stackalloc byte[8];

           
            var currentTerm = BinaryPrimitives.ReadUInt64BigEndian(buffer);    
            var lastApplied = BinaryPrimitives.ReadUInt64BigEndian(buffer.Slice(8));
            record = new RaftMetadata { CurrentTerm = currentTerm, LastAppliedLogEntry = lastApplied };
            return true;
        }


        public int GetLength()
        {
            return 8 + 8;
        }

        public bool TryWrite(ref Span<byte> buffer,out int length)
        {
            length = 16;
            return (BinaryPrimitives.TryWriteUInt64BigEndian(buffer[0..8], CurrentTerm) && BinaryPrimitives.TryWriteUInt64BigEndian(buffer[8..16], LastAppliedLogEntry));

        }

   

        public ulong LastAppliedLogEntry { get; set; }
        public ulong CurrentTerm { get; set; }
    }

    public class WalShardBackend<TCommand, TCommandResult> : IStorageShardBackend<TCommand, TCommandResult>
        where TCommand : ICommand<TCommand>
        where TCommandResult : ICommandResult<TCommandResult>
       
    {
        
        private WriteAheadLog<RaftMetadata> _log;
        private RaftMetadata _metadata;

        private Dictionary<ulong, AsyncOperationWithData<Guid, TCommandResult>> _pendingOperations = new();
        private ObjectPool<AsyncOperationWithData<Guid, TCommandResult>> _operationPool = new DefaultObjectPool<AsyncOperationWithData<Guid, TCommandResult>>(new AsyncOperationWithDataPoolPolicy<Guid, TCommandResult>());

        private object _syncRoot = new object();

        public WalShardBackend(IWALStorageProvider segmentProvider)
        {
            _log = new WriteAheadLog<RaftMetadata>("content", new LogOptions { Storage = segmentProvider });
            _metadata = _log.Metadata?? new RaftMetadata();
        }

        public ulong LastAppliedLogEntry => _metadata.LastAppliedLogEntry;

        public ulong LastLogEntry => _log.GetLastEntryHeader().EntryId;

        public ulong LastLogEntryTerm => _log.GetLastEntryHeader().Term;

        public ulong CurrentTerm => _metadata.CurrentTerm;

        public ShardsConfigurationRecord CurrentShardsConfiguration { get; private set; } = new ShardsConfigurationRecord(null, null);

        public void ApplyEntries(ulong index)
        {
            throw new NotImplementedException();
        }

        public ValueTask<GetEntriesResult> GetEntries(ulong firstEntryId, ulong lastEntryId)
        {
            return _log.GetEntriesAsync(firstEntryId, lastEntryId);
        }

        public bool TryAppendCommand(TCommand command, [NotNullWhen(true)] out LogEntry? entry, [NotNullWhen(false)] out Error? error)
        {
            throw new NotImplementedException();
        }

       

        public bool TryAppendEntries(IEnumerable<LogEntry> entries)
        {
            throw new NotImplementedException();
        }

     
        public bool TryGetEntryTerm(ulong entryId, out ulong entryTerm)
        {
            return _log.TryGetEntryTerm(entryId,out entryTerm);
        }

        public bool TryTruncateEntriesAfter(ulong logEntryId)
        {
            if(LastAppliedLogEntry > logEntryId)
            {
                return false;
            }
            _log.TruncateAfter(logEntryId);
            return true;
        }

        public void UpdateTerm(ulong term)
        {
            if(term != _metadata.CurrentTerm)
            {
                _metadata.CurrentTerm = term;
                _log.UpdateMetadata(_metadata);
            }

        }

        public ValueTask<TCommandResult> WaitCommittedAsync(ulong entryId)
        {
            lock (_syncRoot)
            {
                if (_pendingOperations.TryGetValue(entryId, out var operation))
                {
                    return operation.ValueTaskOfT;

                }
                else
                {
                    return ValueTask.FromException<TCommandResult>(new InvalidOperationException($"No pending operation for {entryId}"));
                }
            }
        }
    }
}
