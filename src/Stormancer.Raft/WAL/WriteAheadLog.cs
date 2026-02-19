using Stormancer.Raft.Storage;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.WAL
{
    public class CreateSnapshotContext<TLogEntry>
    {

    }
    public struct GetEntriesResult : IDisposable
    {
        private readonly IDisposable _disposable;

        internal GetEntriesResult(ulong PrevLogEntryId, ulong PrevLogEntryTerm, ulong FirstEntryId, ulong LastEntryId, IEnumerable<LogEntry> entries, IDisposable disposable)
        {
            this.PrevLogEntryId = PrevLogEntryId;
            this.PrevLogEntryTerm = PrevLogEntryTerm;
            this.FirstEntryId = FirstEntryId;
            this.LastEntryId = LastEntryId;
            Entries = entries;
            _disposable = disposable;
        }

        public ulong PrevLogEntryId { get; }
        public ulong PrevLogEntryTerm { get; }
        public ulong FirstEntryId { get; }
        public ulong LastEntryId { get; }
        public IEnumerable<LogEntry> Entries { get; }

        public void Dispose()
        {
            _disposable.Dispose();
        }
    }
  
    public class LogOptions
    {
        /// <summary>
        /// Maximum number of records in a segment.
        /// </summary>
        public uint MaxSegmentRecordsCount { get; set; } = 100_000;
        public uint MaxRecordSize { get; set; } = 1024 * 1024;
        public uint SegmentSize { get; set; } = 16 * 1024 * 1024;
        public uint PageSize { get; set; } = 8 * 1024;
        public required IWALStorageProvider Storage { get; set; }
    }

    /// <summary>
    /// Error returned by <see cref="WriteAheadLog{TMetadataContent}.TryAppendEntries(IEnumerable{LogEntry}, out AppendEntryError? error)"/>
    /// </summary>
    public class AppendEntryError
    {
        internal AppendEntryError(ErrorId error, LogEntry failedEntry)
        {
            Error = error;
            FailedEntry = failedEntry;
        }

        public ErrorId Error { get; }

        public LogEntry FailedEntry { get; }
    }

    /// <summary>
    /// A write ahead log implementation (WAL)
    /// </summary>
    /// <typeparam name="TMetadataContent">Type representing metadata stored in the WAL</typeparam>
    public class WriteAheadLog<TMetadataContent> : IAsyncDisposable
        where TMetadataContent : IRecord<TMetadataContent>
    {
        private object _lock = new object();
        private readonly string _category;
        private readonly LogOptions _options;
        private IWALSegment _currentSegment;
        private readonly WalMetadata<TMetadataContent> _metadata;

        private IWALStorageProvider _segmentProvider;
        private Dictionary<int, IWALSegment> _openedSegments = new Dictionary<int, IWALSegment>();

        public WriteAheadLog(string category, LogOptions options)
        {
            if (options.Storage == null)
            {
                throw new ArgumentNullException("LogOptions.Storage cannot be null");
            }
            _category = category;
            _options = options;

            _segmentProvider = _options.Storage;
            _metadata = LoadMetadata();
            _currentSegment = GetOrLoadSegment(_metadata.CurrentSegmentId);

        }


        public void UpdateMetadata(TMetadataContent metadataContent)
        {
            _metadata.Content = metadataContent;
            SaveMetadata();
        }

        public TMetadataContent? Metadata => _metadata.Content;

        public LogEntryHeader GetLastEntryHeader()
        {
            return _currentSegment.GetLastEntryHeader();
        }

        public bool TryGetEntryTerm(ulong entryId, out ulong term)
        {
            return _metadata.TryGetTerm(entryId, out term);
        }

        public void TruncateAfter(ulong entryId)
        {


            lock (_lock)
            {
                foreach (var segment in _metadata.RemoveAfter(entryId))
                {
                    if (_currentSegment.SegmentId == segment)
                    {
                        _currentSegment = GetOrLoadSegment(segment - 1);
                    }
                    DeleteSegment(segment);
                }

                _currentSegment.TryTruncateEnd(entryId);

                SaveMetadata();
            }
        }

        private void DeleteSegment(int segmentId)
        {
            if (_currentSegment.SegmentId == segmentId)
            {
                throw new InvalidOperationException("Cannot delete current segment.");
            }
            if (_openedSegments.Remove(segmentId, out var segment))
            {
                segment.Delete();
            }
        }

        /// <summary>
        /// Deletes content, ensuring that records after entryId included are kept alive.
        /// </summary>
        /// <param name="entryId"></param>
        /// <remarks>Content before entry id might not be deleted, the algorithm only </remarks>
        /// <returns></returns>
        public void TruncateBefore(ulong entryId)
        {
            lock (_lock)
            {
                foreach (var segment in _metadata.RemoveBefore(entryId))
                {
                    DeleteSegment(segment);
                }

                SaveMetadata();
            }
        }

        /// <summary>
        /// Creates a snapshot representing the state of the system until entryId, using the provided snapshot generation algorithm
        /// </summary>
        /// <param name="entryId"></param>
        /// <param name="snapshotGenerator"></param>
        /// <returns></returns>
        public ValueTask CreateSnapshot<TLogEntry>(ulong entryId, Func<CreateSnapshotContext<TLogEntry>, ValueTask> snapshotGenerator)
        {
            throw new NotImplementedException("Snapshot");
        }

        public async ValueTask<GetEntriesResult> GetEntriesAsync(ulong firstEntryId, ulong lastEntryId)
        {
            if (firstEntryId < 1)
            {
                firstEntryId = 1;
            }
            if (lastEntryId < firstEntryId)
            {
                lastEntryId = firstEntryId;
            }

            ulong prevLogEntryId;
            ulong prevLogEntryTerm;
            if (firstEntryId > 1)
            {
                if (!_metadata.TryGetSegment(firstEntryId - 1, out var prevEntrySegmentId))
                {
                    throw new NotImplementedException("previous entry not in the log, use snapshot?");
                }
                var prevEntrySegment = GetOrLoadSegment(prevEntrySegmentId);
                var header = await prevEntrySegment.GetEntryHeader(firstEntryId - 1);
                prevLogEntryId = header.EntryId;
                prevLogEntryTerm = header.Term;

            }
            else
            {
                prevLogEntryId = 0;
                prevLogEntryTerm = 0;
            }

            if (!_metadata.TryGetSegment(firstEntryId, out var firstEntrySegmentId))
            {
                throw new NotImplementedException("first entry not in the log, use snapshot?");
            }
            var segment = GetOrLoadSegment(firstEntrySegmentId);

            var result = await segment.GetEntries(firstEntryId, lastEntryId);

            return new GetEntriesResult(prevLogEntryId, prevLogEntryTerm, result.FirstEntryId, result.LastEntryId, result.Entries, result);



        }

        public bool TryAppendEntry(LogEntry entry, [NotNullWhen(false)] out Error? error)
        {
            var segment = GetCurrentSegment();
            int offset = 0;

            while (!TryAppendEntry(segment, entry, ref offset, out error))
            {
                if (error == new Error(WalErrors.SegmentFull,null))
                {
                    segment = CreateNewSegmentIfRequired(segment);
                }
                else if (error == new Error(WalErrors.ContentPartialWrite,null))
                {
                    //Keep writing.
                }
                else
                {
                    return false;
                }
            }
            return true;
        }

        public bool TryAppendEntries(IEnumerable<LogEntry> logEntries,[NotNullWhen(false)] out AppendEntryError? error)
        {
            var segment = GetCurrentSegment();
            foreach (var entry in logEntries)
            {
                int offset = 0;
                Error? e;
                while (!TryAppendEntry(segment, entry, ref offset, out e))
                {
                    if (e.Id == WalErrors.SegmentFull)
                    {
                        segment = CreateNewSegmentIfRequired(segment);
                    }
                    else if (e.Id == WalErrors.ContentPartialWrite)
                    {
                        //Keep writing.
                    }
                    else
                    {
                        error = new AppendEntryError(e.Id, entry);
                        return false;
                    }
                }
            }
            error = null;
            return true;
        }



        private bool TryAppendEntry(IWALSegment segment, LogEntry entry, ref int offset, [NotNullWhen(false)] out Error? error)
        {

            if(segment.GetLastEntryHeader().EntryId != entry.Id -1)
            {
                error = new Error(WalErrors.NonConsecutiveEntryId,null);
                return false;
            }
            if (segment.TryAppendEntry(entry, ref offset, out error))
            {
                if (_metadata.TryAddEntry(segment.SegmentId, entry.Id, entry.Term))
                {
                    SaveMetadata();
                }
                return true;
            }
            else
            {
                return false;
            }



        }

        private IWALSegment CreateNewSegmentIfRequired(IWALSegment expectedSegment)
        {

            if (_currentSegment != expectedSegment)
            {
                return _currentSegment;
            }

            lock (_lock)
            {
                if (_currentSegment != expectedSegment)
                {
                    return _currentSegment;
                }
                else
                {

                    var segment = _currentSegment;
                    segment.SetReadOnly();

                    _currentSegment = GetOrLoadSegment(_metadata.CurrentSegmentId);

                    return _currentSegment;
                }

            }

        }

        private IWALSegment GetCurrentSegment()
        {
            return _currentSegment;
        }



        private void SaveMetadata()
        {
            lock (_lock)
            {

                _segmentProvider.SaveMetadata(_metadata);
            }

        }

        private WalMetadata<TMetadataContent> LoadMetadata()
        {
            if (_segmentProvider.TryReadMetadata<TMetadataContent>(out var metadata))
            {
                return metadata;
            }
            else
            {
                return new WalMetadata<TMetadataContent>();
            }
        }

        private IWALSegment GetOrLoadSegment(int segmentId)
        {

            if (_openedSegments.TryGetValue(segmentId, out var segment))
            {
                return segment;
            }
            else
            {
                if (segmentId == -1)
                {
                    segmentId = 0;
                }

                segment = _segmentProvider.GetOrCreateSegment(_category, segmentId);

                _openedSegments.Add(segmentId, segment);
                return segment;
            }
        }


        public async ValueTask DisposeAsync()
        {
            foreach (var (id, segment) in _openedSegments)
            {
                await segment.DisposeAsync();
            }

            await _currentSegment.DisposeAsync();
        }
    }



    public class WALSegmentState
    {
        internal int UseCounter;
    }

    public struct WalSegmentGetEntriesResult : IDisposable
    {
        public WalSegmentGetEntriesResult(IEnumerable<LogEntry> entries, ulong firstEntryId, ulong lastEntryId, WALSegmentState state)
        {
            Entries = entries;
            FirstEntryId = firstEntryId;
            LastEntryId = lastEntryId;
            _state = state;

            Interlocked.Increment(ref _state.UseCounter);
        }

        private readonly WALSegmentState _state;

        public IEnumerable<LogEntry> Entries { get; }
        public ulong FirstEntryId { get; }
        public ulong LastEntryId { get; }

        public void Dispose()
        {
            Interlocked.Decrement(ref _state.UseCounter);
        }
    }

    public interface IWALSegment : IAsyncDisposable
    {
        int SegmentId { get; }

        string Category { get; }

        ValueTask<WalSegmentGetEntriesResult> GetEntries(ulong firstEntry, ulong lastEntry);

        /// <summary>
        /// Appends an entry in the log. If the entry is too long to be included entirely in the page, offset contains the offset in the entry of the 
        /// </summary>
        /// <param name="logEntry"></param>
        /// <param name="offset"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        bool TryAppendEntry(LogEntry logEntry, ref int offset, [NotNullWhen(false)] out Error? error);

        ValueTask<LogEntryHeader> GetEntryHeader(ulong firstEntry);

        bool TryTruncateEnd( ulong newLastEntryId);
        void SetReadOnly();
        LogEntryHeader GetLastEntryHeader();
        void Delete();

    }

    public struct LogEntryHeader
    {
        public static LogEntryHeader NotFound => new LogEntryHeader { EntryId = 0, Length = 0, Term = 0 };
        public bool Found => Term != 0;
        public required ulong Term { get; init; }
        public required ulong EntryId { get; init; }

        public required int Length { get; init; }
    }
}
