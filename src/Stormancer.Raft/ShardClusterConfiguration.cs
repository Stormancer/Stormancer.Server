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
    public class Server : IEquatable<Server>
    {
        public Server(Guid uid, byte[] data)
        {
            Uid = uid;
            Data = data;
        }

        public Server(Guid uid)
        {
            Uid = uid;
            Data = Array.Empty<byte>();
        }

        public Guid Uid { get; }

        public byte[] Data { get; }

        public override int GetHashCode()
        {
            return Uid.GetHashCode();
        }

        public override bool Equals(object? obj)
        {
            return base.Equals(obj);
        }

        public bool Equals(Server? other)
        {
            if (other == null) return false;
            return other.Uid == Uid;
        }

        public int GetLength()
        {
            return 16 + 4 + Data.Length;
        }

        public bool TryWrite(Span<byte> buffer, out int bytesWritten)
        {
            if (buffer.Length < GetLength())
            {
                bytesWritten = 0;
                return false;
            }

            Uid.TryWriteBytes(buffer[0..16]);
            BinaryPrimitives.TryWriteInt32BigEndian(buffer[16..20], Data.Length);
            Data.CopyTo(buffer[20..]);
            bytesWritten = 20 + Data.Length;
            return true;
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out Server? server, out int bytesRead)
        {
            if (buffer.Length < 20)
            {
                server = null;
                bytesRead = 0;
                return false;
            }

            var reader = new SequenceReader<byte>(buffer.Slice(16, 4));

            reader.TryReadBigEndian(out int length);

            var guidBuffer = buffer.Slice(0, 16);

            Guid guid;
            if (guidBuffer.IsSingleSegment)
            {
                guid = new Guid(guidBuffer.FirstSpan);
            }
            else
            {
                ReadOnlySpan<byte> span = stackalloc byte[16];
                guid = new Guid(span);
            }

            byte[] data = new byte[length];

            buffer.Slice(20, length).CopyTo(data);
            server = new Server(guid, data);
            bytesRead = length + 20;
            return true;
        }

        public static bool TryRead(ref ReadOnlySpan<byte> buffer, [NotNullWhen(true)] out Server? server, out int bytesRead)
        {
            if (buffer.Length < 20)
            {
                server = null;
                bytesRead = 0;
                return false;
            }

            Guid guid = new Guid(buffer[0..16]);

            BinaryPrimitives.TryReadInt32BigEndian(buffer[16..20], out int length);


            byte[] data = new byte[length];

            buffer.Slice(20, length).CopyTo(data);
            server = new Server(guid, data);
            bytesRead = length + 20;
            return true;
        }
    }

    public record ShardsConfigurationRecord(HashSet<Server>? Old, HashSet<Server>? New) : IRecord<ShardsConfigurationRecord>
    {
        private HashSet<Server>? _servers;

        public IEnumerable<Server> All
        {
            get
            {
                if (_servers == null)
                {
                    _servers = new HashSet<Server>();

                    if (Old != null)
                    {
                        foreach (var server in Old)
                        {
                            _servers.Add(server);
                        }
                    }

                    if (New != null)
                    {
                        foreach (var server in New)
                        {
                            _servers.Add(server);
                        }
                    }
                }
                return _servers;
            }
        }

        public bool IsVoting(Guid shardUid)
        {
            var server = new Server(shardUid);
            if (Old == null && New == null)
            {
                return true;
            }
            else if (Old != null && Old.Contains(server))
            {
                return true;
            }
            else if (New != null && New.Contains(server))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public int GetLength()
        {
            // 1 : flags | 2 : oldLength | 2: newLength | Old array | new array |
            var length = 5;
            if (Old != null)
            {
                foreach (var server in Old)
                {
                    length += server.GetLength();
                }
            }
            if (New != null)
            {
                foreach (var server in New)
                {
                    length += server.GetLength();
                }
            }

            return length;
        }

        public bool TryWrite(ref Span<byte> buffer, out int l)
        {
            l = GetLength();
            if (buffer.Length < GetLength())
            {

                return false;
            }

            buffer[0] = (byte)((Old != null ? 1 : 0) | (New != null ? 2 : 0));

            if (Old != null)
            {
                BinaryPrimitives.WriteInt16BigEndian(buffer.Slice(1), (short)Old.Count);

            }
            if (New != null)
            {
                BinaryPrimitives.WriteInt16BigEndian(buffer.Slice(3), (short)New.Count);
            }

            var offset = 5;
            if (Old != null)
            {
                foreach (var server in Old)
                {
                    if (!server.TryWrite(buffer[offset..], out var length))
                    {
                        return false;
                    }
                    else
                    {
                        offset += length;
                    }
                }
            }

            if (New != null)
            {
                foreach (var server in New)
                {
                    if (!server.TryWrite(buffer[offset..], out var length))
                    {
                        return false;
                    }
                    else
                    {
                        offset += length;
                    }
                }
            }

            return true;
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out ShardsConfigurationRecord? config, out int bytesRead)
        {
            var reader = new SequenceReader<byte>(buffer);
            if (!reader.TryRead(out var flags))
            {
                config = default;
                bytesRead = 0;
                return false;
            }
            HashSet<Server>? old = (flags & 1) != 0 ? new HashSet<Server>() : null;
            HashSet<Server>? @new = (flags & 2) != 0 ? new HashSet<Server>() : null;

            short oldCount = 0;
            short newCount = 0;

            if (!reader.TryReadBigEndian(out oldCount))
            {
                config = default;
                bytesRead = 0;
                return false;
            }


            if (!reader.TryReadBigEndian(out newCount))
            {
                config = default;
                bytesRead = 0;
                return false;
            }



            var offset = 5;
            if (old != null)
            {

                for (short i = 0; i < oldCount; i++)
                {
                    if (!Server.TryRead(buffer.Slice(offset), out var server, out var read))
                    {
                        config = default;
                        bytesRead = 0;
                        return false;
                    }
                    else
                    {
                        offset += read;
                        old.Add(server);
                    }
                }
            }
            if (@new != null)
            {
                for (int i = 0; i < newCount; i++)
                {
                    if (!Server.TryRead(buffer.Slice(offset), out var server, out var read))
                    {
                        config = default;
                        bytesRead = 0;
                        return false;
                    }
                    else
                    {
                        offset += read;
                        @new.Add(server);
                    }
                }
            }

            bytesRead = offset;
            config = new ShardsConfigurationRecord(old, @new);
            return true;

        }
    }


}
