using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Stormancer.Raft.WAL;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Stormancer.Raft.Tests
{
    public class IncrementCommand : ICommand<IncrementCommand>
    {
        public Guid Id { get; } = Guid.NewGuid();


        public IRecord Record { get; }

        private IncrementCommand(IRecord systemRecord)
        {
            Record = systemRecord;
        }

        public static IncrementCommand Create(IRecord systemRecord)
        {
            return new IncrementCommand(systemRecord);
        }

        public TSystemCommandContent? As<TSystemCommandContent>() where TSystemCommandContent : class, IRecord<TSystemCommandContent>
        {
            return Record as TSystemCommandContent;
        }
    }
    public class IncrementCommandResult : ICommandResult<IncrementCommandResult>
    {
        private IncrementCommandResult(Guid operationId, bool success, Error? error, int newValue, int entryId)
        {
            OperationId = operationId;
            Success = success;
            Error = error;
            NewValue = newValue;
            EntryId = entryId;
        }
        public Guid OperationId { get; }

        public bool Success { get; }

        public Error? Error { get; }

        public int NewValue { get; }
        public int EntryId { get; }

        public static IncrementCommandResult CreateFailed(Guid operationId, Error error)
        {
            return new IncrementCommandResult(operationId, false, error, 0, 0);
        }

        public static bool TryRead(ReadOnlySequence<byte> buffer, out int bytesRead, [NotNullWhen(true)] out IncrementCommandResult? result)
        {
            throw new NotImplementedException();
        }

        public int GetLength()
        {
            throw new NotImplementedException();
        }

        public void Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
    public class OfflineRaftTests
    {
        [Fact]
        public async Task ElectLeader()
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
            using var logger = new NullLoggerFactory();
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
            var config = new ReplicatedStorageShardConfiguration {ReaderWriter = readerWriter };
            var backend = new WalShardBackend<IncrementCommand, IncrementCommandResult>(provider);
            var channel = new TestMessageChannel(() => 0);
            var shard = new ReplicatedStorageShard<IncrementCommand, IncrementCommandResult>(Guid.Parse("62cb3c01-c215-4225-be3b-7e2d6c85c708"), config, logger, null, backend);
            Assert.True(await shard.ElectAsLeaderAsync());


        }

        [Fact]
        public async Task ExecuteCommand()
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
            using var logger = new NullLoggerFactory();
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };
            var backend = new WalShardBackend<IncrementCommand, IncrementCommandResult>(provider);
            var channel = new TestMessageChannel(() => 0);
            var shard = new ReplicatedStorageShard<IncrementCommand, IncrementCommandResult>(Guid.Parse("62cb3c01-c215-4225-be3b-7e2d6c85c708"), config, logger, null, backend);
            await shard.ElectAsLeaderAsync();
            //var cmd = new IncrementCommand();
            //var result = await shard.ExecuteCommand(cmd);
            //Assert.True(cmd.Id == result.OperationId);
            //Assert.True(result.Success);
        }
    }
}
