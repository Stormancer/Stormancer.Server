using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    public static class RaftErrors
    {
        /// <summary>
        /// No primary shard available to process the operation.
        /// </summary>
        public static ErrorId NotAvailable { get; } = Errors.Register(1_503, "raft.primaryShardNotAvailable", "No primary shard available to process the operation.");

        
    }
}
