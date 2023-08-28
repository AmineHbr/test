using Nest;
using System;

namespace Vector.Infrastructure.ElasticSearch.Services
{
    public class VectorElasticClient : ElasticClient
    {
        public VectorElasticClient(ElasticOptions options) : base(new ConnectionSettings(new Uri(options.ClusterUrl))
                                .BasicAuthentication(options.UserName, options.Password)
                                .DefaultFieldNameInferrer(p => p.ToLower())
                                .PrettyJson().DisableDirectStreaming().EnableApiVersioningHeader())
        {
        }
    }
}
