using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    public interface IRecord
    {
        int GetLength();

        bool TryWrite(ref Span<byte> buffer,out int length);
    }
    public interface IRecord<T> : IRecord where T : IRecord<T>
    {

        static abstract bool TryRead(ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out T? record, out int length);
        static abstract bool TryRead(ReadOnlySequence<byte> buffer,[NotNullWhen(true)] out T? record, out int length);
    }

    public class IntegerTypedRecordLog<TRecord>(int typeId) :IIntegerRecordTypeLogEntryFactory where TRecord : IRecord<TRecord>
    {
        public IEnumerable<(int Id, Type RecordType)> GetMetadata()
        {
            yield return (typeId,typeof(TRecord));
        }

        public bool TryRead(ulong id, ulong term, int recordType, ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out IRecord? entry, out int length)
        {
            if(recordType != typeId)
            {
                length = 0;
                entry = null;
                return false;
            }

         
            if (TRecord.TryRead(buffer, out var record, out length))
            {
                
                entry = record;
                return true;
            }
            else
            {
                entry = null;
                return false;
            }
        }

        public bool TryRead(ulong id, ulong term, int recordType, ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out IRecord? entry, out int length)
        {
            if (recordType != typeId)
            {
                length = 0;
                entry = null;
                return false;
            }


            if (TRecord.TryRead(ref buffer, out var record, out length))
            {
                entry = record;
                return true;
            }
            else
            {
                entry = null;
                return false;
            }
        }

        public bool TryWriteContent(ref Span<byte> buffer, IRecord record, out int length)
        {
            if(record is TRecord)
            {
                return record.TryWrite(ref buffer, out length);
            }
            else
            {
                length = 0;
                return false;
            }
        }
    }
}
