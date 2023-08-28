using RestEase;
using System.Threading.Tasks;

namespace Vector.Batches.Elastic.Infrastructure.External
{
    public interface IVectorApi
    {
        [Header("Content-Type", "application/json")]
        [Post("api/cache/invalidate")]
        Task InvalidateCacheAsync();
    }
}
