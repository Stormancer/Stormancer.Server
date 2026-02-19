using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft
{
    internal static partial class ShardsReplicationLogging
    {

        [LoggerMessage(Level = LogLevel.Trace, Message = "Starting processing command {commandId} of type {type}")]
        public static partial void LogStartProcessingCommand(ILogger logger, Guid commandId, Type type);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Completed processing command {commandId}")]
        public static partial void LogCompleteProcessingCommand(ILogger logger, Guid commandId);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Sending append entry command from {leader} to {replica} term={currentTerm} {firstEntry}=>{lastEntry} | commit={commit}")]
        public static partial void LogSendingAppendCommand(ILogger logger,Guid leader, Guid replica, ulong currentTerm, ulong firstEntry, ulong lastEntry,ulong commit );

        [LoggerMessage(Level = LogLevel.Trace, Message = "Received append entry result from {origin}. Success={success} Term={term} LastEntry={lastEntry} commit={committed}")]
        public static partial void LogReceivedAppendEntriesResult(ILogger logger,bool success, Guid origin,ulong term, ulong lastEntry, ulong committed);

        [LoggerMessage(Level =LogLevel.Trace,Message = "Sending response to AppendEntries from {origin}=>{destination} : {success}, leaderUid={leader} term={term} lastEntry={lastEntry} applied={applied}")]
        public static partial void LogSendingAppendCommandResponse(ILogger logger,Guid origin, Guid destination, bool success, Guid? leader, ulong term, ulong lastEntry, ulong applied);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Sending request vote to {destination}. Current term {currentTerm}, lastEntry={lastLogEntry}:{lastLogEntryTerm}")]
        public static partial void SendingRequestVote(ILogger logger, Guid destination, ulong currentTerm, ulong lastLogEntry, ulong lastLogEntryTerm);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Sending request vote response from {origin} to {destination}. voteGranted {voteGranted} to {votedFor} Current term {currentTerm}, lastEntry={lastLogEntry}:{lastLogEntryTerm},")]
        public static partial void SendingRequestVoteResult(ILogger logger, Guid origin, Guid destination, bool voteGranted, Guid? votedFor, ulong currentTerm, ulong lastLogEntry, ulong lastLogEntryTerm);

        [LoggerMessage(Level =LogLevel.Trace,Message ="Election Complete, success={success}, the new leader is {leader} and received {votes} votes out of {total}.")]
        public static partial void ElectionCompleted(ILogger logger, Guid leader,bool success, int votes, int total);



        [LoggerMessage(Level = LogLevel.Trace, Message = "Completed command {commandId}. term={term},entryId={entryId}")]
        public static partial void CompletedCommand(ILogger logger, Guid commandId, ulong entryId, ulong term);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Failed to commit entries. Last committed={committed} lastApplied={lastApplied},lastLogEntry={lastLogEntry} {synchronized}/{total} replica synchronized.")]
        public static partial void LogFailedLocalCommitAttempt(ILogger logger, ulong committed,ulong lastApplied,ulong lastLogEntry, int synchronized, int total);

        [LoggerMessage(Level = LogLevel.Trace, Message = "Sucessfully committed entries locally. Last committed={committed}, {synchronized}/{total} replica synchronized.")]
        public static partial void LogSuccessfulLocalCommitAttempt(ILogger logger, ulong committed, int synchronized, int total);
    }
}
