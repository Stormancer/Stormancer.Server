using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    public struct LogEntryId : IEquatable<LogEntryId>, IComparable<LogEntryId>
    {
        public LogEntryId(ulong term, ulong entryId)
        {
            Term = term;
            EntryId = entryId;


        }
        public ulong Term { get; }
        public ulong EntryId { get; }

        public bool Equals(LogEntryId other)
        {
            return Term == other.Term && EntryId == other.EntryId;
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj != null && obj is LogEntryId value)
            {
                return Equals(value);
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Term, EntryId);
        }

        public uint GetLength() => 16;

        public static bool TryRead(ReadOnlySequence<byte> buffer, out LogEntryId result, out uint bytesRead)
        {
            bytesRead = 16;
            if(buffer.Length < bytesRead)
            {
                result = default;
                return false;
            }
            ulong term;
            ulong entryId;
            if(buffer.FirstSpan.Length >= 8)
            {
                term = BinaryPrimitives.ReadUInt64BigEndian(buffer.FirstSpan);
            }
            else
            {
                Span<byte> b = stackalloc byte[16];
                buffer.CopyTo(b);
                term = BinaryPrimitives.ReadUInt64BigEndian(b.Slice(0,8));
                entryId = BinaryPrimitives.ReadUInt64BigEndian(b.Slice(8,8));

                result = new LogEntryId(term, entryId);
                return true;
            }


            buffer = buffer.Slice(8);

            if(buffer.FirstSpan.Length >=8)
            {
                entryId = BinaryPrimitives.ReadUInt64BigEndian(buffer.FirstSpan.Slice(0,8));
            }
            else
            {
                Span<byte> b = stackalloc byte[8];
                buffer.CopyTo(b);
                entryId = BinaryPrimitives.ReadUInt64BigEndian(b);
            }
            result = new LogEntryId(term, entryId); 
            return true;
        }

        public bool TryWrite(ref Span<byte> buffer,out uint bytesWritten)
        {
            bytesWritten = 16;
            if(buffer.Length < 16)
            {
                return false;
            }
            else
            {
                BinaryPrimitives.WriteUInt64BigEndian(buffer.Slice(0, 8), Term);
                BinaryPrimitives.WriteUInt64BigEndian(buffer.Slice(8,8), EntryId);
                return true;
            }
        }

        public int CompareTo(LogEntryId other)
        {
            if(Term != other.Term)
            {
                return (int)(Term - other.Term);
            }
            else
            {
                return (int)(EntryId - other.EntryId);
            }
        }
    }
}
