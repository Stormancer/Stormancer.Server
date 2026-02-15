using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.ObjectPool;
using Stormancer.Threading;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

    public class WalShardBackend : IStorageShardBackend   
    {
      
        private readonly WriteAheadLog<RaftMetadata> _log;
        private readonly RaftMetadata _metadata;
        private readonly object _pendingOperationLock = new object();
        private AsyncOperationWithData<(Guid, ulong), RaftCommandResult>? _firstPendingOperation;
        private AsyncOperationWithData<(Guid, ulong), RaftCommandResult>? _lastPendingOperation;
        private static readonly DefaultObjectPool<AsyncOperationWithData<(Guid,ulong), RaftCommandResult>> _defaultObjectPool = new (new AsyncOperationWithDataPoolPolicy<(Guid, ulong), RaftCommandResult>());
        
        private readonly object _syncRoot = new object();

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

       

        public ValueTask<GetEntriesResult> GetEntries(ulong firstEntryId, ulong lastEntryId)
        {
            return _log.GetEntriesAsync(firstEntryId, lastEntryId);
        }

        public ValueTask<RaftCommandResult> TryAppendCommand(RaftCommand command)
        {
            var header = _log.GetLastEntryHeader();
            var e = new LogEntry(header.EntryId + 1, _metadata.CurrentTerm, command.Record);
            
            if (_log.TryAppendEntry(e, out var error))
            {
               
                
                
                return AddOperation(command.Id, e.Id);
            }
            else
            {
                return ValueTask.FromResult(new RaftCommandResult { Error = error });
            }
        }
        private ValueTask<RaftCommandResult> AddOperation(Guid operationId, ulong logEntryId)
        {
            var asyncOp = _defaultObjectPool.Get();
            asyncOp.TryOwnAndReset();
            asyncOp.Item = (operationId, logEntryId);

            lock (_pendingOperationLock)
            {
                if (_lastPendingOperation != null)
                {
                    _lastPendingOperation.Next = asyncOp;
                    _lastPendingOperation = asyncOp;
                }
                else
                {
                    _firstPendingOperation = asyncOp;
                    _lastPendingOperation = asyncOp;
                }
            }

            return asyncOp.ValueTaskOfT; 
        }
       

        public bool TryAppendEntries(IEnumerable<LogEntry> entries,[NotNullWhen(false)] out Error? error)
        {
           
            if (_log.TryAppendEntries(entries, out var e))
            {
                error = null;
                return true;
            }
            else
            {
                error = new Error(e.Error, null);
                return false;
            }
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
        public void ApplyEntries(ulong index)
        {
            if (index > _metadata.LastAppliedLogEntry)
            {
                _metadata.LastAppliedLogEntry = index;
                _log.UpdateMetadata(_metadata);

            }
        }
        public ValueTask<RaftCommandResult> WaitCommittedAsync(ulong entryId)
        {
            lock (_syncRoot)
            {
                if (_pendingOperations.TryGetValue(entryId, out var operation))
                {
                    if(operation.IsCompleted)
                    {
                        _pendingOperations.Remove(entryId);
                        var r = operation.ValueTaskOfT.Result;
                        _defaultObjectPool.Return(operation);
                        return ValueTask.FromResult(r);
                    }
                    else
                    {
                        return operation.ValueTaskOfT;
                    }
                 

                }
                else
                {
                    return ValueTask.FromException<RaftCommandResult>(new InvalidOperationException($"No pending operation in progress for {entryId}"));
                }
            }
        }
    }
}
