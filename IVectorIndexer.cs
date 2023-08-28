using System.Collections.Generic;
using System.Threading.Tasks;

namespace Vector.Batches.Elastic.Infrastructure.ElasticSearch
{
    public interface IVectorIndexer
    {
        Task<string> GetLiveIndexName(string aliasName);

        Task SetLiveIndex(string indexName, string aliasName);

        Task DeleteNonLiveIndices(Dictionary<string, string> indicesDict);

        Task CreateIndexIfNotExists<T>(string indexName) where T : class;

        Task DeleteIndexAsync(string indexName);

        Task BulkIndexData<T>(string indexName, IEnumerable<T> data) where T : class;

        Task<T> GetLastIndexedDocumentByProperty<T>(string indexName, string propertyName) where T : class;
    }
}