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
using Xunit.Abstractions;


namespace Stormancer.Raft.Tests
{

    public class OfflineRaftTests
    {
        private readonly ILoggerFactory _loggerFactory;

        public OfflineRaftTests(ITestOutputHelper outputHelper)
        {
            _loggerFactory = new TestLoggerFactory(outputHelper);
        }
        private Guid GetId(int i) => new Guid(i, 0, 0, new byte[8]);

        [Fact]
        public async Task ElectLeader()
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();

            var db = new MockDatabase();
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };
            var backend = new WalShardBackend("backend",provider, db, _loggerFactory);
            var channel = new TestMessageChannel(() => 0);
            var shard = new ReplicatedStorageShard(GetId(0), config, _loggerFactory, null, backend);
            Assert.True(await shard.ElectAsLeaderAsync());


        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(150)]
        public async Task ExecuteCommand(int count)
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
            using var logger = new NullLoggerFactory();
            var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };
            var db = new MockDatabase();
            var backend = new WalShardBackend("backend",provider,db,_loggerFactory);
            var channel = new TestMessageChannel(() => 0);
            var shard = new ReplicatedStorageShard(GetId(0), config, logger, null, backend);
            await shard.ElectAsLeaderAsync();

            for (var i = 0; i < count; i++)
            {
                var cmd = RaftCommand.Create(new MockRecord { Value = 4 });
                var result = shard.ExecuteCommand(cmd);
                await shard.WaitCommitted(result);
                Assert.True(cmd.Id == result.OperationId);
                Assert.True(result.Success);
            }
        }
    }
}
