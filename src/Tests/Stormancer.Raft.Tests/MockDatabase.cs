using Stormancer.Raft.WAL;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    internal class MockDatabase : IWalDatabase
    {
        public bool TryApplyRecord(IRecord record)
        {
            if(record is MockRecord r)
            {
                Value += r.Value;
                return true;
            }
            else
            {
                return false;
            }
        }

        public int Value { get; private set; }
    }
}
