using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Vector.Infrastructure.ElasticSearch.Services;
using Vector.Infrastructure.Extensions;

namespace Vector.Infrastructure.ElasticSearch
{
    public static class Extensions
    {
        private const string SectionName = "elastic";

        public static IServiceCollection AddVectorElasticClient(this IServiceCollection builder, string sectionName = SectionName)
        {
            if (string.IsNullOrWhiteSpace(sectionName))
            {
                sectionName = SectionName;
            }

            ServiceProvider provider = builder.BuildServiceProvider();

            var options = provider.GetService<IConfiguration>().GetOptions<ElasticOptions>(sectionName);
            return builder.AddVectorElasticClient(options);
        }

        public static IServiceCollection AddVectorElasticClient(this IServiceCollection builder, ElasticOptions options)
        {
            builder.AddSingleton(options);
            builder.AddTransient<IVectorElasticService, VectorElasticService>();
            return builder;
        }
    }
}
