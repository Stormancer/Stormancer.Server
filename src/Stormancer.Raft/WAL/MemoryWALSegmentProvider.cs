using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.WAL
{

    public class MemoryWALSegmentOptions
    {

        public int PageSize { get; init; } = 1024;
        public int PagesPerSegment { get; init; } = 256;

        public MemoryPool<byte> MemoryPool { get; init; } = MemoryPool<byte>.Shared;

        public required ILogEntryReaderWriter ReaderWriter { get; init; }
    }

    class MemoryWALReadOnlySequenceSegment : ReadOnlySequenceSegment<byte>
    {
        public MemoryWALReadOnlySequenceSegment(ReadOnlyMemory<byte> memory, ReadOnlySequenceSegment<byte> next, int runningIndex)
        {
            Memory = memory;
            Next = next;
            RunningIndex = runningIndex;
        }
    }

    public class MemoryWALSegmentProvider : IWALStorageProvider
    {
        private readonly MemoryWALSegmentOptions _options;

        private object _lock = new object();

        private IMemoryOwner<byte>? _metadataMemOwner;
        private Memory<byte>? _metadataBuffer;

        public MemoryWALSegmentProvider(MemoryWALSegmentOptions options)
        {
            _options = options;
        }
        public IWALSegment GetOrCreateSegment(string category, int segmentId)
        {
            return new MemoryWALSegment(category, segmentId, _options);
        }

        public bool TryReadMetadata<TMetadataContent>([NotNullWhen(true)] out WalMetadata<TMetadataContent>? metadata) where TMetadataContent : IRecord<TMetadataContent>
        {
            lock (_lock)
            {

                if (_metadataBuffer != null)
                {
                    return WalMetadata<TMetadataContent>.TryRead(new ReadOnlySequence<byte>(_metadataBuffer.Value), out metadata, out var _);
                }
                else
                {
                    metadata = null;
                    return false;
                }
            }
        }

        public void SaveMetadata<TMetadataContent>(WalMetadata<TMetadataContent> metadata) where TMetadataContent : IRecord<TMetadataContent>
        {
            lock (_lock)
            {
                if (_metadataMemOwner != null)
                {
                    _metadataMemOwner.Dispose();
                }
                _metadataMemOwner = _options.MemoryPool.Rent(metadata.GetLength());

                var buffer = _metadataMemOwner.Memory.Span;

                if (metadata.TryWrite(ref buffer, out var length))
                {
                    _metadataBuffer = _metadataMemOwner.Memory.Slice(0, length);
                }
                else
                {
                    _metadataMemOwner.Dispose();
                    throw new InvalidOperationException("Failed to write metadata.");
                }

            }
        }

        private class MemoryPage : IDisposable, IBufferWriter<byte>
        {

            private readonly IMemoryOwner<byte> _pageBufferOwner;



            public MemoryPage(int id, IMemoryOwner<byte> pageBufferOwner)
            {
                Id = id;
                _pageBufferOwner = pageBufferOwner;
                Size = _pageBufferOwner.Memory.Length;

            }

            public int Size { get; }
            public int Offset { get; private set; } = 0;
            public int Remaining => Size - Offset;

            public int Id { get; }

            public void Advance(int count)
            {
                if (Offset + count > Size)
                {
                    throw new ArgumentException("Cannot advance past the end of the page.");
                }
                Offset += count;
            }

            public void Dispose()
            {
                _pageBufferOwner.Dispose();
            }

            public Memory<byte> GetMemory(int sizeHint = 0)
            {

                if (Offset + sizeHint > Size)
                {
                    throw new ArgumentException("sizeHint too large.");
                }
                if (sizeHint <= 0)
                {
                    sizeHint = Size - Offset;

                }

                return _pageBufferOwner.Memory.Slice(Offset, sizeHint);
            }

            public Span<byte> GetSpan(int sizeHint = 0)
            {
                if (Offset + sizeHint > Size)
                {
                    throw new ArgumentException("sizeHint too large.");
                }

                if (Offset + sizeHint > Size || sizeHint <= 0)
                {
                    sizeHint = Size - Offset;

                }
                return _pageBufferOwner.Memory.Span.Slice(Offset, sizeHint);
            }

            public ReadOnlyMemory<byte> GetContent(int offset, int length)
            {
                if (offset + length > Offset)
                {
                    throw new ArgumentException("offset+length> Offset");
                }
                var span = _pageBufferOwner.Memory.Slice(offset, length);
                return span;
            }

            public bool CanAllocate(int length)
            {
                return Offset + length <= Size;
            }
        }
        private class MemoryWALSegment : IWALSegment
        {
            [Flags]
            private enum IndexRecordFlags : byte
            {
                None = 0,
                RecordStart = 1,
                RecordEnd = 2
            }
            private struct IndexRecord
            {
                public required ulong Term { get; set; }
                public required ulong EntryId { get; set; }
                public required int ContentPageId { get; set; }
                public required int ContentOffset { get; set; }
                public required int ContentLength { get; set; }

                public required IndexRecordFlags Flags { get; set; }

                public const int Length = 8 + 8 + 4 + 4 + 4 + 1;

                public static bool TryRead(ref ReadOnlySpan<byte> buffer, out IndexRecord value)
                {
                    if (buffer.Length < Length)
                    {
                        value = default;
                        return false;
                    }

                    value = new IndexRecord()
                    {
                        Term = BinaryPrimitives.ReadUInt64BigEndian(buffer[0..8]),
                        EntryId = BinaryPrimitives.ReadUInt64BigEndian(buffer[8..16]),
                        ContentPageId = BinaryPrimitives.ReadInt32BigEndian(buffer[16..20]),
                        ContentOffset = BinaryPrimitives.ReadInt32BigEndian(buffer[20..24]),
                        ContentLength = BinaryPrimitives.ReadInt32BigEndian(buffer[24..28]),
                        Flags = (IndexRecordFlags)buffer[28]
                    };
                    return true;
                }

                public bool TryWrite(Span<byte> span)
                {
                    if (span.Length < Length)
                    {
                        return false;
                    }

                    BinaryPrimitives.WriteUInt64BigEndian(span[0..8], Term);
                    BinaryPrimitives.WriteUInt64BigEndian(span[8..16], EntryId);
                    BinaryPrimitives.WriteInt32BigEndian(span[16..20], ContentPageId);
                    BinaryPrimitives.WriteInt32BigEndian(span[20..24], ContentOffset);
                    BinaryPrimitives.WriteInt32BigEndian(span[24..28], ContentLength);
                    span[28] = (byte)Flags;
                    return true;
                }
            }

            [MemberNotNullWhen(true, nameof(_firstRecord), nameof(_lastRecord))]
            private bool HasRecords()
            {
                return _firstRecord != null;
            }

            private object _syncRoot = new object();

            private IndexRecord? _firstRecord;
            private IndexRecord? _lastRecord;

            private WALSegmentState _segmentState = new WALSegmentState();
            private bool _readOnly;
            private List<MemoryPage> _indexPages = new List<MemoryPage>();
            private List<MemoryPage> _contentPages = new List<MemoryPage>();
            private MemoryPage _currentIndexPage;
            private MemoryPage _currentContentPage;


            private readonly MemoryWALSegmentOptions _options;

            public MemoryWALSegment(string category, int segmentId, MemoryWALSegmentOptions options)
            {
                Category = category;
                SegmentId = segmentId;
                _options = options;

                TryCreateNewContentPage();
                TryCreateNewIndexPage();
            }

            [MemberNotNull(nameof(_currentIndexPage))]
            private bool TryCreateNewIndexPage()
            {
                var id = _currentIndexPage != null ? _currentIndexPage.Id + 1 : 0;
                _currentIndexPage = new MemoryPage(id, _options.MemoryPool.Rent(_options.PageSize));
                _indexPages.Add(_currentIndexPage);
                return true;
            }

            [MemberNotNull(nameof(_currentContentPage))]
            private bool TryCreateNewContentPage()
            {
                var id = _currentContentPage != null ? _currentContentPage.Id + 1 : 0;
                _currentContentPage = new MemoryPage(id, _options.MemoryPool.Rent(_options.PageSize));
                _contentPages.Add(_currentContentPage);
                return true;
            }
            public int PagesCount => _indexPages.Count + _contentPages.Count;

            public int SegmentId { get; }

            public string Category { get; }

            public bool IsEmpty => throw new NotImplementedException();

            public ValueTask DisposeAsync()
            {
                Delete();

                return ValueTask.CompletedTask;
            }

            public ValueTask<WalSegmentGetEntriesResult> GetEntries(ulong firstEntryId, ulong lastEntryId)
            {
                if (!HasRecords())
                {
                    return ValueTask.FromResult(new WalSegmentGetEntriesResult(Enumerable.Empty<LogEntry>(), 0, 0, _segmentState));
                }

                if (firstEntryId < _firstRecord.Value.EntryId)
                {
                    firstEntryId = _firstRecord.Value.EntryId;
                }
                if (lastEntryId > _lastRecord.Value.EntryId)
                {
                    lastEntryId = _lastRecord.Value.EntryId;
                }

                if (firstEntryId > lastEntryId)
                {
                    return ValueTask.FromResult(new WalSegmentGetEntriesResult(Enumerable.Empty<LogEntry>(), 0, 0, _segmentState));
                }

                if (!TryGetIndexPosition(firstEntryId, out var firstEntryPageId, out var firstEntryOffset))
                {
                    return ValueTask.FromResult(new WalSegmentGetEntriesResult(Enumerable.Empty<LogEntry>(), 0, 0, _segmentState));
                }

                if (!TryGetIndexPosition(lastEntryId, out var lastEntryPageId, out var lastEntryOffset))
                {
                    return ValueTask.FromResult(new WalSegmentGetEntriesResult(Enumerable.Empty<LogEntry>(), 0, 0, _segmentState));
                }




                IEnumerable<LogEntry> EnumerateEntries(ulong firstEntryId, ulong lastEntryId)
                {
                    if (!TryGetEntryHeader(firstEntryId, out int pageId, out int offset, out var header))
                    {
                        yield break;
                    }


                    while (header.EntryId <= lastEntryId)
                    {

                        if (TryGetLogEntry(header, out LogEntry? entry))
                        {
                            yield return entry;
                        }
                        if(!TryAdvanceIndex(ref pageId, ref offset))
                        {
                            break;
                        }
                        if (!TryGetEntryHeader(pageId, offset, out header))
                        {
                            break;
                        }
                    }



                }

                return ValueTask.FromResult(new WalSegmentGetEntriesResult(EnumerateEntries(firstEntryId, lastEntryId), firstEntryId, lastEntryId, _segmentState));

            }

            private bool TryAdvanceIndex(ref int pageId, ref int offset)
            {
                if(pageId >= _indexPages.Count)
                {
                    return false;
                }

                var page = _indexPages[pageId];
                var recordLength = IndexRecord.Length;
                if (page.Offset >= offset+2*recordLength)
                {
                    offset += recordLength;
                    return true;
                }
                else //change page
                {
                    if(pageId+1 >= _indexPages.Count)
                    {
                        return false;
                    }
                    page = _indexPages[pageId+1];

                    if(page.Offset < recordLength)
                    {
                        return false;
                    }

                    pageId++;
                    offset = 0;
                    return true;
                }

            }

          
            /// <summary>
            /// Tries getting a log entry from an header object. If the entry spans multiple records, advances pageId & offset to the last record of the entry.
            /// </summary>
            /// <param name="header"></param>
            /// <param name="pageId"></param>
            /// <param name="offset"></param>
            /// <returns></returns>
            /// <exception cref="InvalidOperationException"></exception>
            private bool TryGetLogEntry(IndexRecord header,[NotNullWhen(true)] out LogEntry? entry)
            {
                //Need to implement support for entries on multiple records
                
                if(!header.Flags.HasFlag(IndexRecordFlags.RecordStart)) //if not a start record, we should skip
                {
                    entry = null;
                    return false;
                }

                if (header.Flags.HasFlag(IndexRecordFlags.RecordEnd)) //Single segment
                {
                    var contentPage = _contentPages[header.ContentPageId];

                    var content = contentPage.GetContent(header.ContentOffset, header.ContentLength);
                
                    ReadOnlySequence<byte> contentBuffer = new(content);

                    if (_options.ReaderWriter.TryRead(header.EntryId, header.Term, contentBuffer, out entry))
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else //multiple segments.
                {
                    throw new NotImplementedException("multisegment content not supported");
                    
                    //ReadOnlySequenceSegment<byte> firstSegment = new MemoryWALReadOnlySequenceSegment<byte>()
                    //ReadOnlySequenceSegment<byte> lastSegment;


                    //int lastIndex;
                    //ReadOnlySequence<byte> contentBuffer = new(firstSegment, 0, lastSegment, lastIndex);
                    //if (_options.ReaderWriter.TryRead(header.EntryId, header.Term, contentBuffer, out entry))
                    //{
                    //    return true;
                    //}
                    //else
                    //{
                    //    return false;
                    //}

                }

            }


            /// <summary>
            /// Tries to get the pageId and offset to read to get an entry in the index.
            /// </summary>
            /// <param name="entryId"></param>
            /// <param name="pageId"></param>
            /// <param name="offset"></param>
            /// <returns></returns>
            private bool TryGetIndexPosition(ulong entryId, out int pageId, out int offset)
            {
                if (_firstRecord == null)
                {
                    pageId = 0;
                    offset = 0;
                    return false;
                }
                var length = IndexRecord.Length;
                var entriesPerPage = _options.PageSize / length;
                var firstEntryInSegment = _firstRecord.Value;
                if (firstEntryInSegment.EntryId > entryId)
                {
                    pageId = 0;
                    offset = 0;
                    return false;
                }

                if (_lastRecord != null && entryId > _lastRecord.Value.EntryId)
                {
                    pageId = 0;
                    offset = 0;
                    return false;
                }

                MemoryPage? currentPage = null;

                foreach (var page in _indexPages) //Find page
                {
                    if (currentPage == null)
                    {
                        currentPage = page;
                    }
                    else
                    {
                        var span = page.GetContent(0, IndexRecord.Length).Span;
                        if (!IndexRecord.TryRead(ref span, out var indexRecord) || indexRecord.EntryId > entryId)
                        {
                            break;
                        }
                        else
                        {

                            currentPage = page;
                        }
                    }
                }
                if (currentPage == null)
                {
                    pageId = 0;
                    offset = 0;
                    return false;
                }


                for (offset = 0; offset < currentPage.Size; offset += IndexRecord.Length)
                {
                    var span = currentPage.GetContent(offset, IndexRecord.Length).Span;
                    if (IndexRecord.TryRead(ref span, out var indexRecord) && indexRecord.EntryId == entryId)
                    {
                        pageId = currentPage.Id;
                        return true;
                    }
                }

                pageId = 0;
                offset = 0;
                return false;


            }

            private bool TryGetEntryHeader(int pageId, int offset, out IndexRecord header)
            {
                if (_indexPages.Count < pageId)
                {
                    header = default;
                    return false;
                }
                var page = _indexPages[pageId];

                if (offset + IndexRecord.Length > page.Size)
                {
                    header = default;
                    return false;
                }

                var span = page.GetContent(offset, IndexRecord.Length).Span;
                if (IndexRecord.TryRead(ref span, out var record))
                {
                    header = record;
                    return true;
                }
                else
                {
                    header = default;
                    return false;
                }
            }

            private bool TryGetEntryHeader(ulong entryId, out int pageId, out int offset, out IndexRecord header)
            {
                if (TryGetIndexPosition(entryId, out pageId, out offset) && pageId < _indexPages.Count && TryGetEntryHeader(pageId, offset, out header))
                {
                    return true;
                }
                else
                {
                    header = default;
                    return false;
                }

            }
            public ValueTask<LogEntryHeader> GetEntryHeader(ulong entryId)
            {
                if (TryGetEntryHeader(entryId, out _, out _, out var header))
                {
                    return ValueTask.FromResult(new LogEntryHeader { EntryId = header.EntryId, Term = header.Term, Length = header.ContentLength });
                }
                else
                {
                    return ValueTask.FromException<LogEntryHeader>(new InvalidOperationException($"entry {entryId} not found"));
                }
            }

            public bool TryAppendEntry(LogEntry logEntry, ref int contentOffset, [NotNullWhen(false)] out ErrorId? error)
            {
                var writer = _options.ReaderWriter;
                var totalLength = writer.GetContentLength(logEntry);

                var remainingLength = totalLength - contentOffset;

                lock (_syncRoot)
                {
                    if (_readOnly)
                    {
                        error = WalErrors.SegmentReadOnly;
                        return false;
                    }

                    if(remainingLength > _options.PageSize)
                    {
                        error = WalErrors.ContentTooBig;
                        return false;
                    }
                    if (!_currentContentPage.CanAllocate(remainingLength)) //If current page is full, create new page.
                    {
                        if (!TryCreateNewContentPage())
                        {
                            error = WalErrors.SegmentFull;
                            return false;
                        }
                    }


                    while (!_currentIndexPage.CanAllocate(IndexRecord.Length))
                    {
                        if (!TryCreateNewIndexPage())
                        {
                            error = WalErrors.SegmentFull;
                            return false;
                        }

                    }
                    var length = remainingLength > _currentContentPage.Remaining ? _currentContentPage.Remaining : remainingLength;
                    var offset = _currentContentPage.Offset;
                    var pageId = _currentContentPage.Id;
                    var span = _currentContentPage.GetSpan(length);


                    if (!writer.TryWriteContent(ref span, logEntry, contentOffset, out var contentBytesWritten))
                    {
                        error = WalErrors.ContentWriteFailed;
                        return false;
                    }
                    remainingLength -= contentBytesWritten;

                    var indexRecord = new IndexRecord
                    {
                        Term = logEntry.Term,
                        EntryId = logEntry.Id,
                        ContentLength = length,
                        ContentOffset = offset,
                        ContentPageId = pageId,
                        Flags = (remainingLength == 0 ? IndexRecordFlags.RecordEnd : IndexRecordFlags.None) | (contentOffset == 0 ? IndexRecordFlags.RecordStart : IndexRecordFlags.None)
                    };


                    span = _currentIndexPage.GetSpan(IndexRecord.Length);
                    if (!indexRecord.TryWrite(span))
                    {
                        error = WalErrors.IndexWriteFailed;
                        return false;
                    }

                    _currentContentPage.Advance(length);
                    _currentIndexPage.Advance(IndexRecord.Length);
                    offset += length;

                    if (_firstRecord == null)
                    {
                        _firstRecord = indexRecord;
                    }
                    _lastRecord = indexRecord;
                    error = null;

                    contentOffset += contentBytesWritten;

                    if (contentOffset != totalLength)
                    {
                        error = WalErrors.ContentPartialWrite;
                        return false;
                    }
                    return true;

                }
            }



            public bool TryTruncateEnd(ulong newLastEntryId)
            {
                lock (_syncRoot)
                {

                    if (!HasRecords())
                    {
                        return true;
                    }

                    if (newLastEntryId >= _lastRecord.Value.EntryId)
                    {
                        return true;
                    }

                    if (newLastEntryId < _firstRecord.Value.EntryId) //New last entry id is before start entry. Delete the segment.
                    {
                        _currentContentPage = _contentPages[0];
                        _currentIndexPage = _indexPages[0];

                        for (int i = 1; i < _contentPages.Count; i++)
                        {
                            _contentPages[i].Dispose();
                        }
                        _contentPages.RemoveRange(1, _contentPages.Count - 1);

                        for (int i = 1; i < _indexPages.Count; i++)
                        {
                            _indexPages[i].Dispose();
                        }
                        _indexPages.RemoveRange(1, _indexPages.Count - 1);

                        _firstRecord = null;
                        _lastRecord = null;
                        _readOnly = false;
                        return true;
                    }

                    if (TryGetEntryHeader(newLastEntryId, out int indexPageId, out int indexOffset, out var header))
                    {
                        while (!header.Flags.HasFlag(IndexRecordFlags.RecordEnd))
                        {
                            if (TryGetEntryHeader(indexPageId, indexOffset + IndexRecord.Length, out var nextRecord))
                            {
                                if (nextRecord.EntryId != newLastEntryId)
                                {
                                    break;
                                }
                                else
                                {
                                    indexOffset = indexOffset + IndexRecord.Length;
                                    header = nextRecord;
                                }
                            }
                            else
                            {
                                indexPageId++;
                                if (indexPageId >= _indexPages.Count)
                                {
                                    break;
                                }
                            }
                        }
                        _lastRecord = header;


                        _currentIndexPage = _indexPages[indexPageId];
                        _currentContentPage = _contentPages[header.ContentPageId];


                        _readOnly = false;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }

            }

            public void SetReadOnly()
            {
                _readOnly = true;
            }

            public LogEntryHeader GetLastEntryHeader()
            {

                return _lastRecord != null ? new LogEntryHeader { EntryId = _lastRecord.Value.EntryId, Term = _lastRecord.Value.Term, Length = _lastRecord.Value.ContentLength } : LogEntryHeader.NotFound;
            }

            public void Delete()
            {
                foreach (var page in _indexPages)
                {
                    page.Dispose();
                }


                foreach (var page in _contentPages)
                {
                    page.Dispose();
                }
            }
        }
    }


}
