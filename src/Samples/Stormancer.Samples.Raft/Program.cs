// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using Stormancer.Raft;
using System.Diagnostics;

return 0;

//using var logger = LoggerFactory.Create(builder =>
//{
//    builder.AddConsole();
//    //builder.AddFilter((LogLevel level) => true);

//});
//var channel = new MessageChannel(100);
//var config = new ReplicatedStorageShardConfiguration { };


//var shards = new List<ShardContainer>();


//ReplicatedStorageShard<IncrementOperation, IncrementResult>? currentPrimary = null;


//async ValueTask<ReplicatedStorageShard<IncrementOperation, IncrementResult>> AddShard()
//{
//    var backend = new IncrementBackend();
//    var shard = new ReplicatedStorageShard<IncrementOperation, IncrementResult>(Guid.NewGuid(), config, logger, channel, backend);
//    channel.AddShard(shard.ShardUid, shard);
//    shards.Add(new ShardContainer(shard, backend));

//    if (currentPrimary == null)
//    {
//        currentPrimary = shard;
//    }

//    await currentPrimary.UpdateClusterConfiguration(shards.Select(c => new Server(c.Shard.ShardUid)));
//    return shard;
//}

//currentPrimary = await AddShard();

//object syncRoot = new object();



//await currentPrimary.ElectAsLeaderAsync();

//await AddShard();

//bool add = false;

//async Task UpdateShardList()
//{
//    while (true)
//    {

//        var c = Console.ReadKey();

//        switch (c.KeyChar)
//        {
//            case 'a':
//                add = !add;
//                break;
//            case '+':
//                await AddShard();

//                break;

//            case '-':

//                var shard = shards.FirstOrDefault(p => p.Shard.ShardUid != currentPrimary.ShardUid);
//                if (shard != null)
//                {
//                    shards.Remove(shard);
//                    await currentPrimary.UpdateClusterConfiguration(shards.Select(c => new Server(c.Shard.ShardUid)));
//                }
//                channel.RemoveShard(shard.Shard.ShardUid);
//                //channel.RemoveShard(shard.Shard.ShardUid);
//                //primaries.Remove(shard);

//                break;
//            case 'l':

//                shard = shards.FirstOrDefault(p => p.Shard.ShardUid != currentPrimary.ShardUid);

//                if (shard != null)
//                {
//                    await shard.Shard.ElectAsLeaderAsync();
//                    currentPrimary = shard.Shard;
//                }


//                break;
//            default:
//                break;
//        }
//    }
//}
//var time = DateTime.UtcNow;

//_ = Task.Run(() => UpdateShardList());
//var start = Stopwatch.GetTimestamp();
//var i = 0;

//async Task RunCommands()
//{
//    while (true)
//    {
//        i++;
//        if (add)
//        {
//            var t1 = currentPrimary.ExecuteCommand(new IncrementOperation(1));
//            var t2 = currentPrimary.ExecuteCommand(new IncrementOperation(1));
//            var t3 = currentPrimary.ExecuteCommand(new IncrementOperation(1));
//            var t4 = currentPrimary.ExecuteCommand(new IncrementOperation(1));
//            await currentPrimary.ExecuteCommand(new IncrementOperation(1));
//        }
//        else
//        {
//            await Task.Delay(1000);
//        }
//    }
//}

//_ = Task.Run(RunCommands);

////Console.WriteLine(result.CurrentValue);
//while (true)
//{
//    i = 0;
//    await Task.Delay(1000);
//    if (Stopwatch.GetElapsedTime(start) > TimeSpan.FromSeconds(1))
//    {
//        i = 0;
//        start = Stopwatch.GetTimestamp();
//        lock (syncRoot)
//        {

//            foreach (var s in shards)
//            {
//                Console.Write($"[{(s.Shard.IsLeader ? "+" : "-")}]{s.Backend.CurrentTerm} {s.Backend.CurrentValue}|");

//            }
//            Console.WriteLine();
//        }

//        //Console.WriteLine($"{backend1.CurrentValue}{DisplayShardStatus(shard1.Status)}|{backend2.CurrentValue}{DisplayShardStatus(shard2.Status)}|{backend3.CurrentValue}{DisplayShardStatus(shard3.Status)}");
//    }


//}


//class ShardContainer : IDisposable
//{
//    public ShardContainer(ReplicatedStorageShard<IncrementOperation, IncrementResult> shard, IncrementBackend backend)
//    {
//        Shard = shard;
//        Backend = backend;
//    }

//    public ReplicatedStorageShard<IncrementOperation, IncrementResult> Shard { get; }
//    public bool Online { get; set; } = true;
//    public IncrementBackend Backend { get; }

//    private CancellationTokenSource _cts = new CancellationTokenSource();

//    public CancellationToken CancellationToken => _cts.Token;
//    public void Dispose()
//    {
//        _cts.Cancel();
//    }
//}

