using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    public class ReplicatedStorageShardConfiguration
    {
        public MemoryPool<byte> MemoryPool { get; init; } = MemoryPool<byte>.Shared;
        public required ILogEntryReaderWriter ReaderWriter { get; init; }
    }
}
