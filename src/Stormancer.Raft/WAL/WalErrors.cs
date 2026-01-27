using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.WAL
{
    public static class WalErrors
    {
        public static ErrorId SegmentFull { get; } = Errors.Register(2_001, "wal.segmentFull", "The current segment is too small to contain all the data.");
      
        public static ErrorId SegmentReadOnly { get; } = Errors.Register(2_002, "wal.readonly", "Failed to write entry: The log segment is readonly.");
        public static ErrorId ContentWriteFailed { get; } = Errors.Register(2_003, "wal.content.WriteFailed", "Failed to write entry content.");
        public static ErrorId IndexWriteFailed { get; } = Errors.Register(2_004, "wal.index.WriteFailed", "Failed to write index entry.");
        public static ErrorId ContentPartialWrite { get; } = Errors.Register(2_005, "wal.content.partial", "Only a part of the content could be written in the WAL. Try again");

        public static ErrorId ContentTooBig { get; } = Errors.Register(2_006,"wal.contentTooBig","Record content too big.");

        public static ErrorId NonConsecutiveEntryId { get; } = Errors.Register(2_007, "wal.nonConsecutiveEntryId", "Failed to add an entry in the log because its id does not follow the id of the last entry.");
    }
}