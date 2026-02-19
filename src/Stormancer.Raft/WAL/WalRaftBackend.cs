using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.ObjectPool;
using Stormancer.Threading;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
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


        public int GetLength()
        {
            return 8 + 8;
        }

        public bool TryWrite(ref Span<byte> buffer, out int length)
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
        //private readonly object _pendingOperationLock = new object();
        //private AsyncOperationWithData<(Guid operationId, ulong term, ulong entryId), RaftCommandResult>? _firstPendingOperation;
        //private AsyncOperationWithData<(Guid operationId, ulong term, ulong entryId), RaftCommandResult>? _lastPendingOperation;
        //private static readonly DefaultObjectPool<AsyncOperationWithData<(Guid operationId, ulong term, ulong entryId), RaftCommandResult>> _defaultObjectPool = new(new AsyncOperationWithDataPoolPolicy<(Guid operationId, ulong term, ulong entryId), RaftCommandResult>());
       
        private readonly ILogger _logger;

        public WalShardBackend(string categoryName,IWALStorageProvider segmentProvider, ILoggerFactory loggerFactory)
        {
            _log = new WriteAheadLog<RaftMetadata>("content", new LogOptions { Storage = segmentProvider });
            _metadata = _log.Metadata ?? new RaftMetadata();
            _logger = loggerFactory.CreateLogger(categoryName);
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

        public RaftCommandResult TryAppendCommand(RaftCommand command)
        {
            var header = _log.GetLastEntryHeader();
            var e = new LogEntry(header.EntryId + 1, _metadata.CurrentTerm, command.Record);

            if (_log.TryAppendEntry(e, out var error))
            {
                return new RaftCommandResult { LogEntryId = e.Id, OperationId = command.Id, Term = e.Term };// AddOperation(command.Id, e.Term, e.Id);
            }
            else
            {
                return new RaftCommandResult { Error = error };
            }
        }
        //private ValueTask<RaftCommandResult> AddOperation(Guid operationId, ulong term, ulong logEntryId)
        //{
        //    var asyncOp = _defaultObjectPool.Get();
           
        //    while(!asyncOp.TryOwnAndReset())
        //    {
        //       asyncOp = _defaultObjectPool.Get();
        //    }

        //    asyncOp.Item = (operationId, term, logEntryId);

        //    lock (_pendingOperationLock)
        //    {
        //        if (_lastPendingOperation != null)
        //        {
        //            _lastPendingOperation.Next = asyncOp;
        //            _lastPendingOperation = asyncOp;

        //        }
        //        else
        //        {
        //            _firstPendingOperation = asyncOp;
        //            _lastPendingOperation = asyncOp;
        //        }
        //    }

        //    return asyncOp.ValueTaskOfT;
        //}


        public bool TryAppendEntries(IEnumerable<LogEntry> entries, [NotNullWhen(false)] out Error? error)
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
            return _log.TryGetEntryTerm(entryId, out entryTerm);
        }

        public bool TryTruncateEntriesAfter(ulong logEntryId)
        {
            if (LastAppliedLogEntry > logEntryId)
            {
                return false;
            }

            _log.TruncateAfter(logEntryId);
            //lock (_pendingOperationLock)
            //{
            //    var current = _firstPendingOperation;
            //    var last = _firstPendingOperation;
            //    while (current != null && current.Item.entryId <= logEntryId)
            //    {
            //        current = (AsyncOperationWithData<(Guid, ulong, ulong), RaftCommandResult>?)current.Next;
            //        if (current != null && current.Item.entryId <= logEntryId)
            //        {
            //            last = current;
            //        }
            //    }
            //    if (last != null)
            //    {
            //        last.Next = null;
            //        _lastPendingOperation = last;
            //    }
            //    while (current != null)
            //    {
            //        current.TrySetResult(new RaftCommandResult { LogEntryId = current.Item.entryId, OperationId = current.Item.operationId, Term = current.Item.term, Error = new Error(RaftErrors.LeaderChanged, null) });
            //        var old = current;
            //        current = (AsyncOperationWithData<(Guid, ulong, ulong), RaftCommandResult>?)current.Next;
            //        old.Next = null;
            //        _defaultObjectPool.Return(old);
            //    }
            //}
            return true;
        }

        public void UpdateTerm(ulong term)
        {
            if (term != _metadata.CurrentTerm)
            {
                _metadata.CurrentTerm = term;
                _log.UpdateMetadata(_metadata);
            }

        }
        public void ApplyEntries(ulong index)
        {
            
            if (index > _metadata.LastAppliedLogEntry)
            {
                var lastAppliedLogEntry = _metadata.LastAppliedLogEntry;


                while (lastAppliedLogEntry < index)
                {
                    //The uncommitted entries should be in memory, so GetEntriesAsync is expected to return synchronously.
                    var result = _log.GetEntriesAsync(lastAppliedLogEntry + 1, index).Result;
                    if (!result.Entries.Any())
                    {
                        index = lastAppliedLogEntry;
                        TryTruncateEntriesAfter(index);
                        break;
                    }
                    foreach(var entry in result.Entries)
                    {
                        if(entry.Record is ShardsConfigurationRecord shardConfigurationRecord)
                        {
                            this.CurrentShardsConfiguration = shardConfigurationRecord;
                        }
                    }
                    lastAppliedLogEntry = result.LastEntryId;

                }


                _metadata.LastAppliedLogEntry = index;
                _log.UpdateMetadata(_metadata);

            }
            //lock (_pendingOperationLock)
            //{
            //    var current = _firstPendingOperation;
            //    while (current != null && current.Item.entryId <= index)
            //    {

            //        current.TrySetResult(new RaftCommandResult { LogEntryId = current.Item.entryId, OperationId = current.Item.operationId, Term = current.Item.term });
            //        ShardsReplicationLogging.CompletedCommand(_logger, current.Item.operationId, current.Item.entryId, current.Item.term);
            //        _firstPendingOperation = (AsyncOperationWithData<(Guid, ulong, ulong), RaftCommandResult>?)(current.Next);
                    
            //        _defaultObjectPool.Return(current);
            //        current = _firstPendingOperation;
            //    }
            //    if (_firstPendingOperation == null)
            //    {
            //        _lastPendingOperation = null;
            //    }
            //}
        }

    }
}
