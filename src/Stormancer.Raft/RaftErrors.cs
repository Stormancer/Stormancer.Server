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

        /// <summary>
        /// Leader out of sync.
        /// </summary>
        public static ErrorId LeaderChanged { get; } = Errors.Register(1_504, "raft.leaderChanged", "The operation was cancelled because the leader is out of sync.");

        /// <summary>
        /// The shard is not the leader
        /// </summary>
        public static ErrorId NotLEader { get; } = Errors.Register(1_502, "raft.notLeader", "The operation is only supported on the leader.");

    }
}
