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
    internal class NoOpRecord : IRecord<NoOpRecord>
    {
        public static NoOpRecord Instance { get; } = new NoOpRecord();

        public static bool TryRead(ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out NoOpRecord? record, out int length)
        {
            record = Instance;
            length = 0;
            return true;
        }

        public static bool TryRead(ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out NoOpRecord? record, out int length)
        {
            record = Instance;
            length = 0;
            return true;
        }

        public int GetLength()
        {
            return 0;
        }

        public bool TryWrite(ref Span<byte> buffer, out int length)
        {
            length = 0;
            return true;
        }
    }
}
