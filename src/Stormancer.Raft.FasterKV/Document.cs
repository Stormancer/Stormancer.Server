using FASTER.core;
using MessagePack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.FasterKV
{

    [MessagePackObject(true)]
    public class Document
    {
        public int Version { get; internal set; }
      
        public required object Content { get; set; }

    }
    internal class EntrySerializer : BinaryObjectSerializer<Document>
    {
        public override void Deserialize(out Document obj)
        {

            obj = MessagePack.MessagePackSerializer.Deserialize<Document>(reader.BaseStream);
        }

        public override void Serialize(ref Document obj)
        {

            MessagePack.MessagePackSerializer.Serialize(writer.BaseStream, obj);
            
        }
    }
}
