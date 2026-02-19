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

    public class OnlineRaftTests
    {
        private readonly TestLoggerFactory _loggerFactory;

        public OnlineRaftTests(ITestOutputHelper output)
        {
            _loggerFactory = new TestLoggerFactory(output);
        }
        private Guid GetId(int i) => new Guid(i, 0, 0, new byte[8]);

        [Theory(Timeout = 4_000)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ElectLeader(int shardCount)
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
          
            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };
            
            var channel = new TestMessageChannel(() => Random.Shared.Next(50) + 10);

            var shards = new ReplicatedStorageShard[shardCount];
            for (int i = 0; i < shardCount; i++)
            {
                var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
                var backend = new WalShardBackend($"{GetId(i)}/backend", provider, _loggerFactory);

                var shard = new ReplicatedStorageShard(GetId(i), config, _loggerFactory, channel, backend);

                channel.AddShard(shard.ShardUid, shard);
                shards[i] = shard;
            }
            foreach (var shard in shards)
            {
                await shard.UpdateClusterConfiguration(shards.Select(s => new Server(s.ShardUid)));
            }

        
            Assert.True(await shards[0].ElectAsLeaderAsync());

            shards.Single(s => s.IsLeader);
            Assert.True(shards.All(s => s.LeaderUid == shards[0].LeaderUid));
            


        }

        [Theory(Timeout = 4_000)]
        [InlineData(1,4)]
        [InlineData(2,4)]
        [InlineData(20,4)]
        public async Task ExecuteCommandFromLeaderSequence(int count, int shardCount)
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();
            
            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };

            var channel = new TestMessageChannel(() => 50);

            var shards = new ReplicatedStorageShard[shardCount];
            for (int i = 0; i < shardCount; i++)
            {
                var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
                var backend = new WalShardBackend($"{GetId(i)}/backend", provider, _loggerFactory);

                var shard = new ReplicatedStorageShard(GetId(i), config, _loggerFactory, channel, backend);

                channel.AddShard(shard.ShardUid, shard);
                shards[i] = shard;
            }
            foreach (var shard in shards)
            {
                await shard.UpdateClusterConfiguration(shards.Select(s => new Server(s.ShardUid)));
            }
            var s = shards[0];

            Assert.True(await s.ElectAsLeaderAsync());


            for (var i = 0; i < count; i++)
            {
                var cmd = RaftCommand.Create(new MockRecord { Value = 4 });
                var result = s.ExecuteCommand(cmd);
                await s.WaitCommitted(result);
                Assert.True(cmd.Id == result.OperationId);
                Assert.True(result.Success);
               
            }
        }

        [Theory(Timeout=4_000)]
        [InlineData(1, 4)]
        [InlineData(2, 4)]
        [InlineData(20, 4)]
        [InlineData(200, 5)]
        public async Task ExecuteCommandFromLeaderParallel(int count, int shardCount)
        {
            var readerWriter = new ReaderWriterBuilder().AddRecordType<MockRecord>().Create();

            var config = new ReplicatedStorageShardConfiguration { ReaderWriter = readerWriter };

            var channel = new TestMessageChannel(() => 50);

            var shards = new ReplicatedStorageShard[shardCount];
            for (int i = 0; i < shardCount; i++)
            {
                var provider = new MemoryWALSegmentProvider(new MemoryWALSegmentOptions { ReaderWriter = readerWriter });
                var backend = new WalShardBackend($"{GetId(i+1)}/backend", provider, _loggerFactory);

                var shard = new ReplicatedStorageShard(GetId(i+1), config, _loggerFactory, channel, backend);

                channel.AddShard(shard.ShardUid, shard);
                shards[i] = shard;
            }
            foreach (var shard in shards)
            {
                await shard.UpdateClusterConfiguration(shards.Select(s => new Server(s.ShardUid)));
            }
            var s = shards[0];

            Assert.True(await s.ElectAsLeaderAsync());

            var tasks = new List<(RaftCommand cmd,Task<bool> result)>();
            for (var i = 0; i < count; i++)
            {
                var cmd = RaftCommand.Create(new MockRecord { Value = 4 });
                tasks.Add((cmd,s.WaitCommitted(s.ExecuteCommand(cmd)).AsTask()));
            }

            await Task.WhenAll(tasks.Select(t=>t.result)); 

            foreach(var (cmd,task) in tasks)
            {
                var result = await task;
                
                Assert.True(result);
            }
        }
    }
}
