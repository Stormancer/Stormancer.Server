using Stormancer.Raft.WAL;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Stormancer.Raft.Tests
{

    public class MockRecord : IRecord<MockRecord>
    {
        public static bool TryRead(ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out MockRecord? record, out int length)
        {
            length = 4;
            if (BinaryPrimitives.TryReadInt32BigEndian(buffer, out var value))
            {

                record = new MockRecord { Value = value };
                return true;

            }
            else
            {
                record = null;
                return false;
            }
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out MockRecord? record, out int length)
        {
            length = 4;
            var reader = new SequenceReader<byte>(buffer);
            if (reader.TryReadBigEndian(out int value))
            {
                record = new MockRecord { Value = value };

                return true;
            }
            else
            {
                record = null;

                return false;
            }
        }

        public int GetLength()
        {
            return 4;
        }

        public bool TryWrite(ref Span<byte> buffer, out int length)
        {
            length = 4;
            if (buffer.Length < 4)
            {

                return false;
            }
            else
            {
                return BinaryPrimitives.TryWriteInt32BigEndian(buffer, Value);
            }
        }

        public int Value { get; set; }
    }
    public class WALTests
    {
        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(150)]
        [InlineData(10_000)]
        public async Task AddEntries(ulong count)
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });
            ulong logEntryId = 1;
            for (ulong i = 1; i <= count; i++)
            {

                Assert.True(wal.TryAppendEntries(Enumerable.Repeat(1, 10).Select(
                _ => new LogEntry(logEntryId++, 1, new MockRecord())), out var error));

            }

            var header = wal.GetLastEntryHeader();

            Assert.True(header.EntryId == 10 * count);

        }

        [Fact]
        public async Task GetLastEntryHeader()
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });

            Assert.True(wal.TryAppendEntries(new[]{
                new LogEntry(1,1, new MockRecord()),
                new LogEntry(2,1,new MockRecord()),
            }, out var error));

            var header = wal.GetLastEntryHeader();

            Assert.True(header.EntryId == 2 && header.Term == 1);

        }

        [Theory]
        [InlineData(1)]
        [InlineData(10_000)]
        [InlineData(1_000_000)]
        public async Task GetEntries(ulong count)
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });

            for (ulong i = 1; i <= count; i++)
            {
                Assert.True(wal.TryAppendEntry(new LogEntry(i, 1, new MockRecord()), out var error));
            }


            using var result = await wal.GetEntriesAsync(1, count);

            Assert.True(result.FirstEntryId == 1 && result.LastEntryId == count);
            ulong nb = 0;
            foreach (var entry in result.Entries)
            {
                nb++;
            }
            Assert.True(nb == count);

        }

        [Fact]
        public async Task NonConsecutiveEntryId()
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });
            Assert.True(wal.TryAppendEntries(Enumerable.Range(1, 99).Select(i => new LogEntry((ulong)i, 1, new MockRecord())), out var error));
            Assert.False(wal.TryAppendEntries(Enumerable.Range(10, 200).Select(i => new LogEntry((ulong)i, 2, new MockRecord())), out error));
        }
        [Fact]
        public async Task PreviousEntryId()
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });


            Assert.True(wal.TryAppendEntries(Enumerable.Range(1, 99).Select(i => new LogEntry((ulong)i, 1, new MockRecord())), out var error));
            Assert.True(wal.TryAppendEntries(Enumerable.Range(100, 200).Select(i => new LogEntry((ulong)i, 2, new MockRecord())), out error));


            var entries = await wal.GetEntriesAsync(100, 200);

            Assert.True(entries.PrevLogEntryId == 99 && entries.PrevLogEntryTerm == 1);
            entries = await wal.GetEntriesAsync(101, 250);
            Assert.True(entries.PrevLogEntryId == 100 && entries.PrevLogEntryTerm == 2);
        }

        [Fact]
        public async Task TruncateAfter()
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });

            for (ulong i = 1; i <= 10_000; i++)
            {
                Assert.True(wal.TryAppendEntry(new LogEntry(i, 1, new MockRecord()), out var error));
            }

            wal.TruncateAfter(100);

            var header = wal.GetLastEntryHeader();
            Assert.True(header.EntryId == 100);
        }

        [Fact]
        public async Task TruncateBefore()
        {
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions
            {
                ReaderWriter = new IntegerTypedRecordReaderWriter([new IntegerTypedRecordLog<MockRecord>(0)])
            });
            await using var wal = new WriteAheadLog<MockRecord>("test", new LogOptions { Storage = provider });

            for (ulong i = 1; i <= 10_000; i++)
            {
                Assert.True(wal.TryAppendEntry(
                 new LogEntry(i, 1, new MockRecord()), out var error));
            }

            wal.TruncateBefore(1000);

            var header = wal.GetLastEntryHeader();
            Assert.True(header.EntryId == 10_000);
        }

    }
}
