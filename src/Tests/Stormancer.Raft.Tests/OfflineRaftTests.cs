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
    
    public class OfflineRaftTests
    {
        [Fact]
        public async Task ElectLeader()
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
            using var logger = new NullLoggerFactory();
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
            var config = new ReplicatedStorageShardConfiguration {ReaderWriter = readerWriter };
            var backend = new WalShardBackend(provider);
            var channel = new TestMessageChannel(() => 0);
            var shard = new ReplicatedStorageShard(Guid.Parse("62cb3c01-c215-4225-be3b-7e2d6c85c708"), config, logger, null, backend);
            Assert.True(await shard.ElectAsLeaderAsync());


        }

        [Fact]
        public async Task ExecuteCommand()
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
            using var logger = new NullLoggerFactory();
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };
            var backend = new WalShardBackend(provider);
            var channel = new TestMessageChannel(() => 0);
            var shard = new ReplicatedStorageShard(Guid.Parse("62cb3c01-c215-4225-be3b-7e2d6c85c708"), config, logger, null, backend);
            await shard.ElectAsLeaderAsync();
            //var cmd = new IncrementCommand();
            //var result = await shard.ExecuteCommand(cmd);
            //Assert.True(cmd.Id == result.OperationId);
            //Assert.True(result.Success);
        }
    }
}
