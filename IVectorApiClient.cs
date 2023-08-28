using System.Threading.Tasks;

namespace Vector.Batches.Elastic.Infrastructure.External
{
    public interface IVectorApiClient
    {
        Task InvalidateCacheAsync();
    }
}
