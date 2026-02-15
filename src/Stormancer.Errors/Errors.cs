using System;
using System.Buffers.Binary;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer
{
    public class ErrorId(int code, string id, string description) : IEquatable<ErrorId>
    {
        public int Code { get; } = code;

        public string Id { get; } = id;

        public string Description { get; } = description;

        public bool Equals(ErrorId? other)
        {
            if (other == null) return false;

            return Code == other.Code;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as ErrorId);
        }

        public override int GetHashCode()
        {
            return Code.GetHashCode();
        }


    }

    public class Error
    {
        public Error(ErrorId id, string? details)
        {
            Id = id;
            var detailsLength = Details != null ? Encoding.UTF8.GetByteCount(Details) : 0;
            if (detailsLength > ushort.MaxValue)
            {
                throw new ArgumentException($"Details must be {ushort.MaxValue} or less bytes long.", nameof(details));
            }
            Details = details;
        }

        public ErrorId Id { get; }

        public string? Details { get; }

        public int GetLength()
        {
            return 4;
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, out Error? error, out int readBytes)
        {
            var reader = new SequenceReader<byte>(buffer);
            if (reader.TryReadBigEndian(out int code) && Errors.TryGet(code, out var errorId))
            {
                error = new Error(errorId, null);
                readBytes = 4;
                return true;
            }
            else
            {
                error = null;
                readBytes = (int)buffer.Length < 4 ? (int)buffer.Length : 4;
                return false;
            }

        }

        public bool TryWrite(Span<byte> buffer, out int writtenBytes)
        {
            if (BinaryPrimitives.TryWriteInt32BigEndian(buffer, this.Id.Code))
            {
                writtenBytes = 4;
                return true;
            }
            else
            {
                writtenBytes = (int)buffer.Length;
                return false;
            }
        }

        public override string ToString() => $"{Id.Id}({Id.Code}) ({Details})";
    }

    public class Errors
    {

        private static Dictionary<int, ErrorId> _errors = new();


        /// <summary>
        /// The error id is not registered.
        /// </summary>
        public static ErrorId UnknownError { get; } = Errors.Register(1, "unknown", "The error id is not registered.");


        public static ErrorId Register(int code, string id, string description)
        {

            var error = new ErrorId(code, id, description);

            _errors.Add(code, error);

            return error;
        }


        public static IReadOnlyDictionary<int, ErrorId> Ids => _errors.AsReadOnly();

        public static bool TryGet(int code, [NotNullWhen(true)] out ErrorId? error)
        {
            return _errors.TryGetValue(code, out error);
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, out int bytesRead, [NotNullWhen(true)] out Error? error)
        {
            var reader = new SequenceReader<byte>(buffer);
            Span<byte> span = stackalloc byte[6];
            if (!reader.TryCopyTo(span))
            {
                error = null;
                bytesRead = 0;
                return false;
            }

            var code = BinaryPrimitives.ReadInt32BigEndian(span.Slice(0, 4));
            var length = BinaryPrimitives.ReadInt16BigEndian(span.Slice(4, 6));
            string? details;
            if (reader.Remaining < length)
            {
                error = null;
                bytesRead = 0;
                return false;
            }

            if (length > 0)
            {

                details = Encoding.UTF8.GetString(buffer.Slice(6, length));
            }
            else
            {
                details = null;
            }
            bytesRead = length + 6;

            if (TryGet(code, out var errorId))
            {
                error = new Error(errorId, details);
            }
            else
            {
                error = new Error(UnknownError, $"{code},{details}");
            }

            return true;

        }

        public static int GetLength(Error error)
        {
            short detailsLength = error.Details != null ? (short)Encoding.UTF8.GetByteCount(error.Details) : (short)0;
            return 6 + detailsLength;
        }
        public static void Write(ref Span<byte> span, Error error)
        {

            short detailsLength = error.Details != null ? (short)Encoding.UTF8.GetByteCount(error.Details) : (short)0;
            if (span.Length < 6 + detailsLength)
            {
                throw new InvalidOperationException($"Buffer is {span.Length} bytes long, {6 + detailsLength} required.");
            }


            BinaryPrimitives.WriteInt32BigEndian(span.Slice(0, 4), error.Id.Code);
            BinaryPrimitives.WriteInt16BigEndian(span.Slice(4, 2), detailsLength);

            if (detailsLength > 0)
            {
                Encoding.UTF8.GetBytes(error.Details, span.Slice(6, detailsLength));
            }

        }
    }
}
