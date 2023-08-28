namespace Vector.Infrastructure.ElasticSearch
{
    public class ElasticOptions
    {
        public string ClusterUrl { get; set; }

        public string CartIndexName { get; set; }

        public string EbookIndexName { get; set; }
        public string GlobalPrimaryIndexName { get; set; }

        public string EbookDealsIndexName { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }
    }
}
