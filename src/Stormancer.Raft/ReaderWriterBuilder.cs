using Stormancer.Raft.WAL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    public class ReaderWriterBuilder
    {
        public ILogEntryReaderWriter Create()
        {
            return  new IntegerRecordTypeLogEntryReaderWriter(_factories);
        }

        public ReaderWriterBuilder AddRecordType<T>() where T : IRecord<T>
        {
            _factories.Add(new IntegerTypedRecordLog<T>(_factories.Count));
            return this;
        }

        private List<IIntegerRecordTypeLogEntryFactory> _factories = new List<IIntegerRecordTypeLogEntryFactory>([new IntegerTypedRecordLog<NoOpRecord>(0), new IntegerTypedRecordLog<ShardsConfigurationRecord>(1)]);
    }
}
