using RestEase;
using Serilog;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace Vector.Batches.Elastic.Infrastructure.External
{
    public class VectorApiClient : IVectorApiClient
    {
        private readonly IVectorApi _vectorApi;
        private readonly ILogger _logger;

        public VectorApiClient(VectorApiOptions options, ILogger logger)
        {
            var httpClientHandler = new HttpClientHandler();
            var httpClient = new HttpClient(httpClientHandler)
            {
                BaseAddress = new Uri(options.ApiBaseUrl)
            };
            _vectorApi = RestClient.For<IVectorApi>(httpClient);
            _logger = logger;
        }


        public async Task InvalidateCacheAsync()
        {
            _logger.Information($"Invalidate vector cache");
            try
            {
                await _vectorApi.InvalidateCacheAsync();
            }
            catch (Exception ex)
            {
                _logger.Information($"Vector cache invalidation error: {ex.Message}");
            }
        }
    }
}
