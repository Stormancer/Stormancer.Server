using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.WAL
{
    public class WalMetadata<TContent> where TContent : IRecord<TContent>
    {
        private readonly object _lock = new object();

        public int Version { get; set; } = 1;

        public int CurrentSegmentId => SegmentsStarts.Count - 1 + SegmentIdOffset;

        private List<ulong> SegmentsStarts { get; } = new List<ulong>();


        public int SegmentIdOffset { get; private set; } = 0;

        /// <summary>
        /// Tries updating the metadata by adding an entry. 
        /// </summary>
        /// <param name="segmentId"></param>
        /// <param name="entry"></param>
        /// <param name="term"></param>
        /// <returns>True if the metadata changed.</returns>
        public bool TryAddEntry(int segmentId, ulong entry, ulong term)
        {
            var result = false;
            if (!(SegmentsStarts.Count + SegmentIdOffset > segmentId))
            {
                SegmentsStarts.Add(entry);
                result |= true;
            }

            if (Terms.Count == 0 || Terms.Last().Term < term)
            {
                result |= true;
                Terms.Add((entry, term));
            }

            return result;

        }
        
        public IEnumerable<int> RemoveBefore(ulong entryId)
        {

            while (SegmentsStarts.Count > 1 && SegmentsStarts[1] < entryId)
            {
                SegmentsStarts.RemoveAt(0);
                yield return SegmentIdOffset;
                SegmentIdOffset++;
            }

            while (Terms.Count > 1 && Terms[1].EntryId < entryId)
            {
                Terms.RemoveAt(0);
            }
        }

        public IEnumerable<int> RemoveAfter(ulong entryId)
        {



            while (SegmentsStarts.Count > 1 && SegmentsStarts[SegmentsStarts.Count - 1] > entryId)
            {
                yield return (SegmentIdOffset + SegmentsStarts.Count - 1);
                SegmentsStarts.RemoveAt(SegmentsStarts.Count - 1);
            }

            while (Terms.Last().EntryId > entryId)
            {
                Terms.RemoveAt(Terms.Count - 1);
            }

        }

        public bool TryGetSegment(ulong entryId, out int segmentId)
        {

            for (int i = SegmentsStarts.Count - 1; i >= 0; i--)
            {
                ulong entry = SegmentsStarts[i];
                if (entry <= entryId)
                {
                    segmentId = i + SegmentIdOffset;
                    return true;
                }
            }

            segmentId = -1;
            return false;

        }


        /// <summary>
        /// Item added to the list whenever an entry is added with a new term.
        /// Provides fast synchronous query for entryId => term queries without needing to load segments.
        /// The list updates only when a new leader is successfully elected, it shouldn't happen often.
        /// </summary>
        public List<(ulong EntryId, ulong Term)> Terms { get; } = new();

        public bool TryGetTerm(ulong entryId, out ulong term)
        {

            for (int i = Terms.Count - 1; i >= 0; i--)
            {
                var termInfo = Terms[i];
                if (termInfo.EntryId <= entryId)
                {
                    term = termInfo.Term;
                    return true;
                }

            }

            term = 0;
            return false;

        }

        public TContent? Content { get; set; }


        public static bool TryRead(ReadOnlySequence<byte> buffer, out WalMetadata<TContent>? metadata, out int length)
        {
            // | VERSION | SEGMENTSTARTS_LEN | TERMS_LEN | SEGMENTID_OFFSET | CONTENT_LEN |   SEGMENTSTARTS      |     TERMS    |   CONTENT   |
            // |    4    |        4          |     4     |        4         |      4      | 8*SEGMENT_STARTS_LEN | 16*TERMS_LEN | CONTENT_LEN |
            if (buffer.Length < 20)
            {
                length = 20;
                metadata = null;
                return false;
            }
            var reader = new SequenceReader<byte>(buffer);

            reader.TryReadBigEndian(out int version);
            reader.TryReadBigEndian(out int segmentStartsLength);
            reader.TryReadBigEndian(out int termsLength);
            reader.TryReadBigEndian(out int segmentIdOffset);
            reader.TryReadBigEndian(out int contentLength);

            length = GetLength(segmentStartsLength, termsLength, contentLength);
            if (buffer.Length < length)
            {
                metadata = null;
                return false;
            }

            metadata = new WalMetadata<TContent> { Version = version, SegmentIdOffset = segmentIdOffset };

            for (int i = 0; i < segmentStartsLength; i++)
            {
                reader.TryReadBigEndian(out long value);
                metadata.SegmentsStarts.Add((ulong)value);
            }

            for (int i = 0; i < termsLength; i++)
            {
                reader.TryReadBigEndian(out long entryId);
                reader.TryReadBigEndian(out long term);
                metadata.Terms.Add(((ulong)entryId, (ulong)term));
            }

            var contentBuffer = buffer.Slice(reader.Consumed);
            TContent.TryRead(contentBuffer, out var content, out _);

            metadata.Content = content;

            return true;
        }

        private static int GetLength(int segmentsLength, int termsLength, int contentLength)
        {
            // | VERSION | SEGMENTSTARTS_LEN | TERMS_LEN | SEGMENTID_OFFSET | CONTENT_LEN |   SEGMENTSTARTS      |     TERMS    |   CONTENT   |
            // |    4    |        4          |     4     |        4         |      4      | 8*SEGMENT_STARTS_LEN | 16*TERMS_LEN | CONTENT_LEN |
            return 4 + 4 + (4 + (segmentsLength * 8)) + (4 + contentLength) + (4 + termsLength * 16);
        }
        public int GetLength()
        {
            return GetLength(SegmentsStarts.Count, Terms.Count, Content?.GetLength() ?? 0);
        }
        public bool TryWrite(ref Span<byte> buffer, out int length)
        {
            length = GetLength();
            if (buffer.Length < length)
            {
                return false;
            }

            // | VERSION | SEGMENTSTARTS_LEN | TERMS_LEN | SEGMENTID_OFFSET | CONTENT_LEN |   SEGMENTSTARTS      |     TERMS    |   CONTENT   |
            // |    4    |        4          |     4     |        4         |      4      | 8*SEGMENT_STARTS_LEN | 16*TERMS_LEN | CONTENT_LEN |

            BinaryPrimitives.WriteInt32BigEndian(buffer[0..4], Version);
            BinaryPrimitives.WriteInt32BigEndian(buffer[4..8], SegmentsStarts.Count);
            BinaryPrimitives.WriteInt32BigEndian(buffer[8..12], Terms.Count);
            BinaryPrimitives.WriteInt32BigEndian(buffer[12..16], SegmentIdOffset);
            BinaryPrimitives.WriteInt32BigEndian(buffer[16..20], Content?.GetLength() ?? 0);

            var offset = 20;
            for (int i = 0; i < SegmentsStarts.Count; i++)
            {
                BinaryPrimitives.WriteUInt64BigEndian(buffer.Slice(offset, 8), SegmentsStarts[i]);
                offset += 8;
            }
            for (int i = 0; i < Terms.Count; i++)
            {
                var term = Terms[i];
                BinaryPrimitives.WriteUInt64BigEndian(buffer.Slice(offset, 8), term.EntryId);

                BinaryPrimitives.WriteUInt64BigEndian(buffer.Slice(offset + 8, 8), term.Term);
                offset += 16;
            }

            return true;
        }


    }
}
