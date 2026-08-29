using FASTER.core;
using Stormancer.Raft.WAL;
using System;

namespace Stormancer.Raft.FasterKV
{
    public class FasterKVDatabase : IWalDatabase
    {
        private readonly FasterKV<string, Document> _store;
        public FasterKVDatabase(FasterKV<string, Document> store)
        {
            _store = store;
        }
        public bool TryApplyRecord(IRecord record)
        {
            _store.For().NewSession();
        }
    }
}
