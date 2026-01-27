using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stormancer.Raft.WAL
{

    /// <summary>
    /// Provides methods to read log entry content
    /// </summary>
    /// <remarks>
    /// This interface provide a way to abstract they way a WALStorage provider stores log entry content, especially if the content's size is too big.
    /// </remarks>
 
    public interface IWALStorageProvider
    {
        IWALSegment GetOrCreateSegment(string category, int segmentId);
        bool TryReadMetadata<TMetadataContent>([NotNullWhen(true)] out WalMetadata<TMetadataContent>? metadata) where TMetadataContent : IRecord<TMetadataContent>;
        void SaveMetadata<TMetadataContent>(WalMetadata<TMetadataContent> metadata) where TMetadataContent : IRecord<TMetadataContent>;
    }
}
