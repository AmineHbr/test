using AutoMapper.Internal;
using Nest;
using Polly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Vector.Application.DTO;
using Vector.Application.DTO.Filters;
using Vector.Application.DTO.Searches;
using Vector.Application.Queries.Ebook;
using Vector.Application.Queries.Primary;
using Vector.Application.Queries.Secondary;
using Vector.Common.Domain.BondRadar;
using Vector.Common.Domain.Cart;
using Vector.Common.Domain.Ebook;
using Vector.Common.Domain.GlobalPrimaryMarket;
using Vector.Infrastructure.Redis;

namespace Vector.Infrastructure.ElasticSearch.Services
{
    public class VectorElasticService : IVectorElasticService
    {
        /// <summary>
        /// timeout for search queries (for getting 1 page of result)
        /// </summary>
        private const int MAX_SEARCH_TIMEOUT = 5;

        private const string SeniorPreferred = "SeniorPreferred";

        private const int LEAGUE_TABLE_SIZE = 10;
        private const string CREDIT_AGRICOLE = "CREDIT AGRICOLE";

        /// <summary>
        /// timeout for non searh queries (widget, filters)
        /// </summary>
        private const int MAX_TIMEOUT = 30;

        private readonly IElasticClient _elasticClient;
        private readonly ElasticOptions _options;
        private readonly IVectorRedisRepository _vectorRedisRepository;

        public VectorElasticService(IElasticClient elasticClient, ElasticOptions options,
            IVectorRedisRepository vectorRedisRepository)
        {
            _elasticClient = elasticClient;
            _options = options;
            _vectorRedisRepository = vectorRedisRepository;
        }

        public async Task<PagedResult<CartIndex>> SearchSecondary(SearchCartQuery request, bool withOnlyExistingSales)
        {
            var commonIncludeQueries = BuildCartQuery(request.IncludedFilters, request.GridFilters, request.SalesUtCode,
                request.TraderIds, withOnlyExistingSales);
            var includeQueries = !request.IsTraded
                ? commonIncludeQueries
                : commonIncludeQueries
                    .Concat(QueryBuilder.MultiMatchPhraseQueries(nameof(CartIndex.RFQStatus).ToLower(),
                        new[] { "Done" })).ToArray();
            var excludeQueries = BuildCartQuery(request.ExcludedFilters, new GridFilter[0], new string[0],
                new string[0], false);

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<CartIndex>>(r => !r.IsValid).RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<CartIndex>(s => s
                        .Index(Nest.Indices.Index(_options.CartIndexName))
                        .Sort(sortDescriptor => SortBuilder.BuildSortDescriptor(request.GridSorting, sortDescriptor))
                        .From(request.StartIndex)
                        .Size(request.EndIndex - request.StartIndex)
                        .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries))));
                });


            if (searchResponse.IsValid)
            {

                return new PagedResult<CartIndex>
                {
                    Items = searchResponse.Documents.ToList(),
                    Count = searchResponse.Total
                };
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<IEnumerable<DailyPotentiallyData>> GetReportData(DateTime startDate, DateTime endDate)
        {
            var queries = BuildReportQuery(startDate, endDate);

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<CartIndex>>(r => !r.IsValid).RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<CartIndex>(s => s
                        .Index(Indices.Index(_options.CartIndexName))
                        .Size(0)
                        .Query(q => q.Bool(b => b.Must(queries)))
                        .Aggregations(a => a
                            .Terms("by_assetClassLvl3", t => t.Field(f => f.AssetClassLvl3.Suffix("keyword"))
                                .Aggregations(aa => aa
                                    .Terms("by_assetClassLvl4", t => t.Field(f => f.AssetClassLvl4.Suffix("keyword"))
                                        .Aggregations(aaa => aaa
                                            .Terms("by_currency", t => t.Field(f => f.Currency.Suffix("keyword"))
                                                .Aggregations(aaaa => aaaa
                                                    .Filter("buy_traded_filter", f => f
                                                        .Filter(ff =>
                                                            ff.Term(t => t.Verb.Suffix("keyword"), "Buy") &&
                                                            ff.Term(t => t.IsTraded, true))
                                                        .Aggregations(aa => aa.Sum("buy_traded",
                                                            s => s.Field(f => f.TradeAmount))))
                                                    .Filter("buy_pot_filter", f => f
                                                        .Filter(ff =>
                                                            ff.Term(t => t.Verb.Suffix("keyword"), "Buy") &&
                                                            ff.Term(t => t.IsPotential, true))
                                                        .Aggregations(aa =>
                                                            aa.Sum("buy_pot", s => s.Field(f => f.TradeAmount))))
                                                    .Filter("sell_traded_filter", f => f
                                                        .Filter(ff =>
                                                            ff.Term(t => t.Verb.Suffix("keyword"), "Sell") &&
                                                            ff.Term(t => t.IsTraded, true))
                                                        .Aggregations(aa => aa.Sum("sell_traded",
                                                            s => s.Field(f => f.TradeAmount))))
                                                    .Filter("sell_pot_filter", f => f
                                                        .Filter(ff =>
                                                            ff.Term(t => t.Verb.Suffix("keyword"), "Sell") &&
                                                            ff.Term(t => t.IsPotential, true))
                                                        .Aggregations(aa =>
                                                            aa.Sum("sell_pot", s => s.Field(f => f.TradeAmount))))
                                                    .Average("nb_trader", d => d.Field(f => f.NumOfDealers))
                                                )))
                                    )
                                ))));
                });


            if (searchResponse.IsValid)
            {
                var aggregationResults = new List<DailyPotentiallyData>();
                foreach (var assetClass3Bucket in searchResponse.Aggregations.Terms("by_assetClassLvl3").Buckets)
                {
                    foreach (var assetClass4Bucket in assetClass3Bucket.Terms("by_assetClassLvl4").Buckets)
                    {
                        foreach (var currencyBucket in assetClass4Bucket.Terms("by_currency").Buckets)
                        {
                            var reportData = new DailyPotentiallyData
                            {
                                AssetClassLvl3 = assetClass3Bucket.Key,
                                AssetClassLvl4 = assetClass4Bucket.Key,
                                Currency = currencyBucket.Key,
                                BidPotVol = currencyBucket.Filter("buy_pot_filter").Sum("buy_pot")?.Value
                                    .GetValueOrDefault() ?? 0,
                                BidTradVol = currencyBucket.Filter("buy_traded_filter").Sum("buy_traded")?.Value
                                    .GetValueOrDefault() ?? 0,
                                OfferPotVol = currencyBucket.Filter("sell_pot_filter").Sum("sell_pot")?.Value
                                    .GetValueOrDefault() ?? 0,
                                OfferTradVol = currencyBucket.Filter("sell_traded_filter").Sum("sell_traded")?.Value
                                    .GetValueOrDefault() ?? 0,
                                AvgNbTraders = currencyBucket.Average("nb_trader")?.Value.GetValueOrDefault() ?? 0,
                            };

                            aggregationResults.Add(reportData);
                        }
                    }
                }

                return aggregationResults;
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<PagedResult<EbookOrderIndex>> SearchPrimary(SearchEbookQuery request)
        {
            var includeQueries = BuildEbookQuery(request.IncludedFilters, request.GridFilters, request.SalesUtCode);
            var excludeQueries = BuildEbookQuery(request.ExcludedFilters, new GridFilter[0], new string[0]);

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<EbookOrderIndex>>(r => !r.IsValid)
                .RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<EbookOrderIndex>(s => s
                        .Index(Nest.Indices.Index(_options.EbookIndexName))
                        .Sort(sortDescriptor => SortBuilder.BuildSortDescriptor(request.GridSorting, sortDescriptor))
                        .From(request.StartIndex)
                        .Size(request.EndIndex - request.StartIndex)
                        .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries))));
                });


            if (searchResponse.IsValid)
            {
                return new PagedResult<EbookOrderIndex>
                {
                    Items = searchResponse.Documents.ToList(),
                    Count = searchResponse.Total
                };
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<PagedResult<EbookDealIndex>> SearchPrimaryDeals(SearchEbookDealsQuery request)
        {
            var includeQueries = BuildEbookDealsQuery(request.IncludedFilters, request.GridFilters);
            var includeNestedQueries =
                BuildEbookDealsNestedQuery(request.IncludedFilters, request.SalesUtCode, nameof(EbookDealIndex.Orders).ToLower());

            var excludeQueries = BuildEbookDealsQuery(request.ExcludedFilters, Array.Empty<GridFilter>());
            var excludeNestedQueries = BuildEbookDealsNestedQuery(request.ExcludedFilters, Array.Empty<string>(), nameof(EbookDealIndex.Orders).ToLower());

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<EbookDealIndex>>(r => !r.IsValid).RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<EbookDealIndex>(s => s
                        .Index(Nest.Indices.Index($"{_options.EbookDealsIndexName}"))
                        .Sort(sortDescriptor => SortBuilder.BuildSortDescriptor(request.GridSorting, sortDescriptor))
                        .From(request.StartIndex)
                        .Size(request.EndIndex - request.StartIndex)
                        .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries)) &&
                                    q.Bool(b => b.Must(x => x.Nested(n => n.Path(f => f.Orders).Query(nq =>
                                            nq.Bool(b => b.Must(includeNestedQueries)))
                                        .InnerHits(ih => ih.Name("innerOrders").Size(100))
                                    )))
                                    && q.Bool(b => b.MustNot(x => x.Nested(n =>
                                        n.Path(f => f.Orders).Query(nq =>
                                            nq.Bool(b => b.Must(excludeNestedQueries))
                                        ).InnerHits(ih => ih.Name("innerOrders").Size(100))
                                    )))));
                });


            if (searchResponse.IsValid)
            {
                return new PagedResult<EbookDealIndex>
                {
                    Items = searchResponse.Hits.Select(h =>
                    {
                        h.Source.Orders = h.InnerHits.ContainsKey("innerOrders")
                            ? h.InnerHits["innerOrders"].Hits.Documents<EbookOrderInfo>()
                            : h.Source.Orders;
                        return h.Source;
                    }).ToList(),
                    Count = searchResponse.Total
                };
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<IEnumerable<TopIssuesDto>> SearchTopIssues(SearchTopIssuesRequestQuery query, string[] salesUtCodes)
        {
            var includeQueries = BuildSearchTopIssuesQuery(query.Request.IncludedFilters);
            var excludeQueries = BuildSearchTopIssuesQuery(query.Request.ExcludedFilters);
            var salesCodeQuery = BuildNestedDealsSalesCodeQuery(salesUtCodes, nameof(EbookDealIndex.Orders).ToLower());

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<GlobalPrimaryDeal>>(r => !r.IsValid)
                .RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<GlobalPrimaryDeal>(s => s
                        .Index(Nest.Indices.Index($"{_options.GlobalPrimaryIndexName}"))
                        .Sort(sort => sort.Descending(p => p.VolumeUsd))
                        .Size(5)
                        .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.Must(x => x.Nested(n => n
                            .Path(f => f.Orders).Query(nq =>
                                nq.Bool(b => b.Must(salesCodeQuery)))
                        ))) && q.Bool(b => b.MustNot(excludeQueries)))
                    );
                });
            if (searchResponse.IsValid)
            {
                var topIssues = searchResponse.Documents.Select(x => new TopIssuesDto
                {
                    AssetClass = x.IssuerSector,
                    Currency = x.Currency,
                    Green = x.IsESG,
                    Issuer = x.IssuerName,
                    Maturity = x.Maturity,
                    Volume = x.Volume,
                    VolumeUsd = x.VolumeUsd,
                    Investors = x.Orders.OrderByDescending(x => x.TradeAmount).Take(5).Select(y => new IssueInvestorDto
                    {
                        TradeAmount = y.TradeAmount,
                        InvestorCountry = y.InvestorCountry,
                        Name = y.InvestorName,
                        Sales = y.SalesName
                    })
                }).ToList();

                return topIssues;
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<BondDescriptionDto> SearchBondDescription(string isin)
        {
            QueryContainer[] includeQueries = QueryBuilder.MultiMatchPhraseQueries(nameof(CartIndex.Isin), new[] { isin });

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<CartIndex>>(r => !r.IsValid).RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<CartIndex>(s => s
                        .Index(_options.CartIndexName)
                        .Query(q => q.Bool(b => b.Must(includeQueries)))
                        .Sort(sort => sort.Descending(p => p.Descr.Suffix("keyword"))
                                          .Descending(p => p.SalesName.Suffix("keyword"))
                                          .Descending(p => p.FitchRating.Suffix("keyword"))
                                          .Descending(p => p.StandardAndPoorsRating.Suffix("keyword"))
                                          .Descending(p => p.MoodysRating.Suffix("keyword"))
                                          .Descending(p => p.IndustrySector.Suffix("keyword"))
                                          .Descending(p => p.PaymentRank.Suffix("keyword"))
                                          .Descending(p => p.AmountOutstanding)
                        ).Size(1)
                    );
                });

            if (searchResponse.IsValid)
            {
                var bondDescription = searchResponse.Documents.Select(x => new BondDescriptionDto
                {
                    BondDescription = x.Descr,
                    FitchRating = x.FitchRating,
                    SpRating = x.StandardAndPoorsRating,
                    MoodyRating = x.MoodysRating,
                    Trader = x.SalesName,
                    IndustrySector = x.IndustrySector,
                    AmountIssued = x.AmountOutstanding,
                    Seniority = x.PaymentRank
                }).FirstOrDefault();

                return bondDescription;
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<IEnumerable<string>> GetAllSecondaryPossibleValues(string propertyName, string query = null)
        {
            return await GetAllPossibleValues<CartIndex>(propertyName, new[] { _options.CartIndexName }, query);
        }

        public async Task<IEnumerable<string>> GetAllPrimaryPossibleValues(string propertyName, string query = null,
            bool isNested = false, string nestedPath = null)
        {
            return await GetAllPossibleValues<GlobalPrimaryDeal>(propertyName, new[] { _options.GlobalPrimaryIndexName },
                query, isNested, nestedPath);
        }

        public async Task<IEnumerable<string>> GetAllEbookPossibleValues(string propertyName, string query = null)
        {
            return await GetAllPossibleValues<EbookOrderIndex>(propertyName, new[] { _options.EbookIndexName }, query);
        }

        public async Task<IEnumerable<string>> AutoCompleteEbookIssuer(string query)
        {
            return await AutoCompleteFieldSuggest<EbookOrderIndex, string>(query, _options.EbookIndexName,
                f => f.IssuerSuggest,
                o => o.Source.Issuer);
        }

        public async Task<IEnumerable<LabelValue>> AutoCompleteCartDescription(string query)
        {
            return await AutoCompleteFieldSuggest<CartIndex, LabelValue>(query, _options.CartIndexName,
                f => f.DescrSuggest, o => new LabelValue { Label = o.Source.Descr, Value = o.Source.Isin });
        }

        public async Task<IEnumerable<string>> AutoCompleteCartIssuer(string query)
        {
            return await AutoCompleteFieldSuggest<CartIndex, string>(query, _options.CartIndexName,
                f => f.IssuerSuggest, o => o.Source.Issuer);
        }

        public async Task<IEnumerable<string>> GetAllPossibleValues<T>(string propertyName, string[] indices,
            string query = null, bool isNested = false, string nestedPath = null) where T : class
        {
            IEnumerable<string> values;
            string cacheKey = $"{string.Join("-", indices)}{propertyName}_filter_{DateTime.Today:yyyy-MM-dd}";

            var cachedItem = string.IsNullOrEmpty(query)
                ? await _vectorRedisRepository.GetItemAsync<IEnumerable<string>>(cacheKey)
                : null;

            if (cachedItem != null)
            {
                values = cachedItem.ToList();
            }
            else
            {
                var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_TIMEOUT);
                var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<T>>(r => !r.IsValid).RetryAsync(3);

                var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                    .ExecuteAsync(async () =>
                    {
                        return await _elasticClient.SearchAsync<T>(s =>
                        {
                            var sd = s.Index(Indices.Index(indices));
                            if (!string.IsNullOrWhiteSpace(query))
                            {
                                var filterQuery = QueryBuilder.MultiMatchPhraseQueries(propertyName, new[] { query });
                                sd = sd.Query(q => q.Bool(b => b.Must(filterQuery)));
                            }

                            if (isNested)
                            {
                                return sd.Size(0) // get aggs only
                                    .Aggregations(aggs => aggs.Nested("nested", n => n.Path(nestedPath)
                                        .Aggregations(nestedAggs => nestedAggs.Terms($"{propertyName}_term",
                                            t => t.Field($"{nestedPath}.{propertyName}.keyword")
                                        .Size(10000)))
                                    ));
                            }

                            return sd.Size(0) // get aggs only
                                .Aggregations(s => s.Terms($"{propertyName}_term",
                                    t => t.Field($"{propertyName}.keyword")
                                .Size(10000)));
                        });
                    });

                if (searchResponse.IsValid)
                {
                    var aggs = searchResponse.Aggregations;
                    if (isNested)
                    {
                        aggs = searchResponse.Aggregations.Nested("nested");
                    }

                    var list = aggs.Terms($"{propertyName}_term").Buckets.Select(b => b.Key).OrderBy(x => x).ToList();
                    values = list.Select(x => x.Trim()).ToList();

                    if (string.IsNullOrWhiteSpace(query))
                    {
                        TimeSpan untilMidnight = DateTime.Today.AddDays(1.0) - DateTime.Now;
                        await _vectorRedisRepository.UpdateItemAsync(cacheKey, values, untilMidnight);
                    }
                }
                else
                {
                    throw new Exception(searchResponse.DebugInformation);
                }
            }

            return values;
        }

        public async Task<IEnumerable<SecondaryFlowDataDto>> GetTopIssuers(GetSecondaryFlowDataQuery query)
        {
            var timeoutPolicy = Polly.Policy.TimeoutAsync(1);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<CartIndex>>(r => !r.IsValid).RetryAsync(3);
            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    var commonIncludeQueries = BuildSecondaryFilterQuery(query.Request.IncludedFilters);
                    var includeQueries = !query.Request.IsTraded
                        ? commonIncludeQueries
                        : commonIncludeQueries
                            .Concat(QueryBuilder.MultiMatchPhraseQueries(nameof(CartIndex.RFQStatus).ToLower(),
                                new[] { "Done" })).ToArray();
                    var excludeQueries = BuildSecondaryFilterQuery(query.Request.ExcludedFilters);
                    return await _elasticClient.SearchAsync<CartIndex>(s => s
                        .Index(Nest.Indices.Index(_options.CartIndexName))
                        .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries)))
                        .Size(0)
                        .Aggregations(agg => agg.Terms("by_issuers",
                            chd => chd.Size(int.MaxValue).Field($"{nameof(CartIndex.Issuer).ToLower()}.keyword")
                                .Aggregations(x =>
                                    x.Sum("sum_nominal", s => s.Field(nameof(CartIndex.TradeAmount).ToLower()))
                                        .BucketSort("sort_sum",
                                            x => x.Sort(s => s.Descending("sum_nominal")).From(0).Size(5))))));
                });


            if (searchResponse.IsValid)
            {
                var topIssuers = searchResponse.Aggregations.Terms("by_issuers").Buckets.Select(b => b.Key).ToList();

                if (topIssuers.Count == 0)
                {
                    return new List<SecondaryFlowDataDto>();
                }

                searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                    .ExecuteAsync(async () =>
                    {
                        query.Request.IncludedFilters.IssuerNames = topIssuers.ToArray();
                        var commonQueryInc = BuildSecondaryFilterQuery(query.Request.IncludedFilters);
                        var queryInc = !query.Request.IsTraded
                            ? commonQueryInc
                            : commonQueryInc.Concat(
                                QueryBuilder.MultiMatchPhraseQueries(nameof(CartIndex.RFQStatus).ToLower(),
                                    new[] { "Done" })).ToArray();
                        var queryExc = BuildSecondaryFilterQuery(query.Request.ExcludedFilters);
                        return await _elasticClient.SearchAsync<CartIndex>(s => s
                            .Index(Nest.Indices.Index(_options.CartIndexName))
                            .Query(q => q.Bool(b => b.Must(queryInc)) && q.Bool(b => b.MustNot(queryExc))).Size(0)
                            .Aggregations(agg =>
                                agg.Terms("by_issuers",
                                    t => t.Size(int.MaxValue).Field(f => f.Issuer.Suffix("keyword")).Aggregations(
                                        chld =>
                                            chld.Terms("by_investor_sell",
                                                    t => t.Size(int.MaxValue)
                                                        .Field(f => f.InvestorName.Suffix("keyword"))
                                                        .Aggregations(agg2 => agg2.Filter("sells",
                                                            f => f.Filter(t => t.Match(f =>
                                                                    f.Field(f => f.Verb.Suffix("keyword"))
                                                                        .Query("Sell")))
                                                                .Aggregations(child => child.Sum("sum_sells",
                                                                    f => f.Field(u => u.TradeAmount)))
                                                        ))
                                                )
                                                .Terms("by_investor_buy",
                                                    t => t.Size(int.MaxValue)
                                                        .Field(f => f.InvestorName.Suffix("keyword"))
                                                        .Aggregations(agg2 => agg2.Filter("buys",
                                                            f => f.Filter(t => t.Match(f =>
                                                                    f.Field(f => f.Verb.Suffix("keyword"))
                                                                        .Query("Buy")))
                                                                .Aggregations(child => child.Sum("sum_buys",
                                                                    f => f.Field(u => u.TradeAmount)))
                                                        ))
                                                )
                                    )
                                )));
                    });

                if (searchResponse.IsValid)
                {
                    var data = searchResponse.Aggregations.Terms("by_issuers").Buckets.Select(td =>
                    {
                        var byInvestorSells = td.Terms("by_investor_sell").Buckets
                            .Select(b => new { b.Key, b.Filter("sells").SumBucket("sum_sells").Value }).ToList();
                        var byInvestorBuys = td.Terms("by_investor_buy").Buckets
                            .Select(b => new { b.Key, b.Filter("buys").SumBucket("sum_buys").Value }).ToList();

                        return new SecondaryFlowDataDto
                        {
                            Issuer = td.Key,
                            Buy = byInvestorBuys.Sum(x => x.Value) ?? 0,
                            Sell = byInvestorSells.Sum(x => x.Value) ?? 0,
                            Details = byInvestorSells.Union(byInvestorBuys).GroupBy(d => d.Key)
                                .OrderByDescending(x => x.Sum(v => v.Value)).Select(t => new SecondaryFlowDataDetailsDto
                                {
                                    Investor = t.Key,
                                    Buy = byInvestorBuys.SingleOrDefault(x => x.Key == t.Key)?.Value ?? 0,
                                    Sell = byInvestorSells.SingleOrDefault(x => x.Key == t.Key)?.Value ?? 0,
                                }).Take(10).ToList()
                        };
                    }).OrderByDescending(x => x.Buy + x.Sell).ToList();

                    return data;
                }
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<IEnumerable<SummaryActivityDataDto>> GetSummaryActivity(GetSummaryActivityDataQuery query)
        {
            var includeQueries = BuildSecondaryFilterQuery(query.Request.IncludedFilters);
            var excludeQueries = BuildSecondaryFilterQuery(query.Request.ExcludedFilters);

            includeQueries = !query.Request.IsTraded
                ? includeQueries
                : includeQueries.Concat(QueryBuilder.MultiMatchPhraseQueries(nameof(CartIndex.RFQStatus).ToLower(),
                    new[] { "Done" })).ToArray();

            var timeoutPolicy = Polly.Policy.TimeoutAsync(1);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<CartIndex>>(r => !r.IsValid).RetryAsync(3);


            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy).ExecuteAsync(async () =>
            {
                return await _elasticClient.SearchAsync<CartIndex>(s => s
                    .Index(Nest.Indices.Index(_options.CartIndexName))
                    .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries)))
                    .Aggregations(agg =>
                        agg.Terms("by_tradedate",
                            t => t.Size(int.MaxValue).Field(f => f.TradeDate.Suffix("keyword"))
                                .Aggregations(child => child.Filter("sell_transactions", f => f.Filter(t =>
                                        query.Request.IsTraded
                                            ? t.Bool(x => x.Must(m => m.Match(f =>
                                                  f.Field(f => f.RFQStatus.Suffix("keyword")).Query("Done"))))
                                              && t.Bool(x => x.Must(m =>
                                                  m.Match(f => f.Field(f => f.Verb.Suffix("keyword")).Query("Sell"))))
                                            : t.Bool(x => x.Must(m =>
                                                m.Match(f => f.Field(f => f.Verb.Suffix("keyword")).Query("Sell"))))))
                                    .Filter("buy_transactions", f => f.Filter(t =>
                                        query.Request.IsTraded
                                            ? t.Bool(x => x.Must(m =>
                                                  m.Match(f =>
                                                      f.Field(f => f.RFQStatus.Suffix("keyword")).Query("Done"))))
                                              && t.Bool(x => x.Must(m =>
                                                  m.Match(f => f.Field(f => f.Verb.Suffix("keyword")).Query("Buy"))))
                                            : t.Bool(x => x.Must(m =>
                                                m.Match(f => f.Field(f => f.Verb.Suffix("keyword")).Query("Buy"))))))
                                )
                        )
                    )
                    .Size(0));
            });


            if (searchResponse.IsValid)
            {
                var data = searchResponse.Aggregations.Terms("by_tradedate").Buckets.Select(td =>
                {
                    return new SummaryActivityDataDto
                    {
                        EventDate = td.Key,
                        NbTransactionsSell = (int)td.Filter("sell_transactions").DocCount,
                        NbTransactionsBuy = (int)td.Filter("buy_transactions").DocCount,
                        NbRfq = (int)td.DocCount,
                    };
                }).ToList();

                return data;
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<IEnumerable<TopInvestorDto>> GetTopInvestors(SearchTopInvestorQuery query)
        {
            var timeoutPolicy = Polly.Policy.TimeoutAsync(1);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<GlobalPrimaryDeal>>(r => !r.IsValid)
                .RetryAsync(3);

            var includeQueries = BuildQueryTopInvestor(query.Request.IncludedFilters, query.Request.IsSeniorPreffered);
            var excludeQueries = BuildQueryTopInvestor(query.Request.ExcludedFilters);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy).ExecuteAsync(async () =>
            {
                return await _elasticClient.SearchAsync<GlobalPrimaryDeal>(s => s
                    .Index(Indices.Index(_options.GlobalPrimaryIndexName))
                    .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries)))
                    .Size(0)
                    .Aggregations(aggs => aggs
                        .Nested("nested_orders", n => n
                            .Path("orders")
                            .Aggregations(nestedAggs => nestedAggs
                                .Terms("investors_terms", t => t
                                    .Script(script =>
                                        script.Source("doc['orders.investorname.keyword'].value.toUpperCase()"))
                                    .Size(20)
                                    .Order(o => o.Descending("total_tradeamount"))
                                    .Aggregations(a => a
                                        .Sum("total_tradeamount", sa => sa
                                            .Field($"orders.{nameof(EbookOrderInfo.TradeAmountUsd).ToLower()}")))
                                )
                            ))
                    ));
            });

            if (searchResponse.IsValid)
            {
                var data = searchResponse.Aggregations.Nested("nested_orders").Terms("investors_terms").Buckets.Select(
                    x => new TopInvestorDto
                    {
                        InvestorName = x.Key,
                        Allocation = x.Sum("total_tradeamount").Value
                    }).ToList();
                return data;
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<IEnumerable<CountryRepartitionDto>> GetCountryRepartition(string isin)
        {
            var timeoutPolicy = Polly.Policy.TimeoutAsync(1);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<EbookOrderIndex>>(r => !r.IsValid)
                .RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy).ExecuteAsync(async () =>
            {
                return await _elasticClient.SearchAsync<EbookOrderIndex>(s => s
                    .Index(Nest.Indices.Index(_options.EbookIndexName))
                    .Query(q => q.Bool(b => b.Must(m => m.Match(f => f.Field(x => x.Isin).Query(isin))))
                                && q.Bool(b => b.Must(m => m.Range(x => x.Field(f => f.TradeAmount).GreaterThan(0)))))
                    .Aggregations(agg =>
                        agg.Terms("by_investor_country",
                            t => t.Size(int.MaxValue).Field(f => f.InvestorCountry.Suffix("keyword"))
                        )
                    )
                    .Size(0));
            });


            if (searchResponse.IsValid)
            {
                var data = searchResponse.Aggregations.Terms("by_investor_country").Buckets.Select(td =>
                {
                    return new CountryRepartitionDto
                    {
                        CountryName = td.Key,
                        Count = (int)td.DocCount,
                    };
                }).ToList();

                return data;
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<PagedResult<GlobalPrimaryDeal>> GetIssues(SearchIssuesQuery query)
        {
            var includeQueries = BuildSearchIssuesQuery(query.Request.IncludedFilters, query.Request.GridFilters);
            var excludeQueries = BuildSearchIssuesQuery(query.Request.ExcludedFilters, Array.Empty<GridFilter>());

            var nestedIncludeQueries = QueryBuilder
                .MultiMatchPhraseQueries(nameof(BondRadarDealBankInfo.BankName).ToLower(), query.Request.IncludedFilters.Leads, "banks")
                .ToArray();

            var nestedExcludeQueries = QueryBuilder
                .MultiMatchPhraseQueries(nameof(BondRadarDealBankInfo.BankName).ToLower(), query.Request.ExcludedFilters.Leads, "banks")
                .ToArray();

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<GlobalPrimaryDeal>>(r => !r.IsValid)
                .RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<GlobalPrimaryDeal>(s => s
                        .Index(Indices.Index(_options.GlobalPrimaryIndexName))
                        .Sort(sortDescriptor => SortBuilder.BuildSortDescriptor(query.Request.GridSorting, sortDescriptor))
                        .From(query.Request.StartIndex)
                        .Size(query.Request.EndIndex - query.Request.StartIndex)
                        .Query(q => q.Bool(b => b.Must(includeQueries))
                                    && q.Bool(b => b.MustNot(excludeQueries))
                                    && q.Nested(n => n.Path("banks")
                                        .Query(nq => nq.Bool(nb => nb.Must(nestedIncludeQueries))
                                                     && nq.Bool(nb => nb.MustNot(nestedExcludeQueries)))))
                        );
                });


            if (searchResponse.IsValid)
            {
                return new PagedResult<GlobalPrimaryDeal>
                {
                    Items = searchResponse.Documents.ToList(),
                    Count = searchResponse.Total
                };
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        public async Task<LeagueTablesDto> GetLeagueTables(SearchLeagueTableQuery query)
        {
            var includeQueries = BuildSearchIssuesQuery(query.Request.IncludedFilters, query.Request.GridFilters, query.Request.IsOnlyEsg);
            var excludeQueries = BuildSearchIssuesQuery(query.Request.ExcludedFilters, Array.Empty<GridFilter>());

            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_SEARCH_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<GlobalPrimaryDeal>>(r => !r.IsValid)
                .RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<GlobalPrimaryDeal>(s => s
                        .Index(Nest.Indices.Index(_options.GlobalPrimaryIndexName))
                        .Size(0)
                        .Query(q => q.Bool(b => b.Must(includeQueries)) && q.Bool(b => b.MustNot(excludeQueries)))
                        .Aggregations(aggs => aggs
                            .Nested("nested_banks", n => n
                                .Path("banks")
                                .Aggregations(nestedAggs => nestedAggs
                                      .Terms("bank_terms", t => t
                                          .Script(script => script.Source("doc['banks.bankname.keyword'].value.toUpperCase()"))
                                          .Size(20)
                                          .Order(o => o.Descending("total_amount"))
                                          .Aggregations(a => a
                                                 .Sum("total_amount", sa => sa
                                                 .Field($"banks.{nameof(BondRadarDealBankInfo.VolumeUsd).ToLower()}")))
                                          )
                                        ))
                            ));
                });


            if (searchResponse.IsValid)
            {
                var data = searchResponse.Aggregations.Nested("nested_banks").Terms("bank_terms").Buckets.Select((x, index) => new LeagueTableRowDto
                {
                    RankBankName = x.Key,
                    Volume = x.Sum("total_amount").Value ?? 0,
                    Deals = x.DocCount,
                    Rank = index
                }).ToList();
                var totalVolume = data.Sum(x => x.Volume);

                var cacib = data.SingleOrDefault(d => d.RankBankName == CREDIT_AGRICOLE);
                var result = data.Take(LEAGUE_TABLE_SIZE).Select(l => new LeagueTableRowDto
                {
                    Rank = l.Rank,
                    Deals = l.Deals,
                    RankBankName = l.RankBankName,
                    Volume = l.Volume,
                    Shares = Math.Round((l.Volume / totalVolume) * 100, 2)
                }).ToList();

                if (!result.Any(r => r.RankBankName == CREDIT_AGRICOLE) && cacib != null)
                {
                    cacib.Shares = Math.Round((cacib.Volume / totalVolume) * 100, 2);
                    result.RemoveAt(result.Count - 1);
                    result.Add(cacib);
                }

                return new LeagueTablesDto(result.OrderBy(r => r.Rank).ToList());
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        private async Task<IEnumerable<U>> AutoCompleteFieldSuggest<T, U>(string query, string index,
            Expression<Func<T, CompletionField>> fieldExpr, Func<ISuggestOption<T>, U> selectFieldResult)
            where T : class where U : class
        {
            var timeoutPolicy = Polly.Policy.TimeoutAsync(MAX_TIMEOUT);
            var retryPolicy = Polly.Policy.HandleResult<ISearchResponse<T>>(r => !r.IsValid).RetryAsync(3);

            var searchResponse = await timeoutPolicy.WrapAsync(retryPolicy)
                .ExecuteAsync(async () =>
                {
                    return await _elasticClient.SearchAsync<T>(s =>
                        s.Index(index)
                            .Suggest(su => su.Completion("suggestions", c => c
                                .Field(fieldExpr)
                                .Prefix(query)
                                .Size(10000))));
                });

            if (searchResponse.IsValid)
            {
                return searchResponse.Suggest["suggestions"].SelectMany(s => s.Options.Select(selectFieldResult))
                    .Distinct().Take(100).ToList();
            }

            throw new Exception(searchResponse.DebugInformation);
        }

        private static QueryContainer[] BuildCartQuery(Application.DTO.Filters.Filter filters, GridFilter[] gridFilters,
            string[] salesUtCode, string[] traderIds, bool withOnlyExistingSales)
        {
            var query = QueryBuilder.ParseCustomFilter<CartIndex>(gridFilters)
                .Concat(BuildSecondaryFilterQuery(filters)).ToArray();

            query = query
                .WithTextFilters(nameof(CartIndex.SalesUtCode), salesUtCode)
                .WithTextFilters(nameof(CartIndex.TraderId), traderIds);

            if (withOnlyExistingSales)
                query.Concat(QueryBuilder.ExistingFieldQuery(nameof(CartIndex.SalesUtCode).ToLower()));

            return query;
        }

        private static QueryContainer[] BuildSearchIssuesQuery(Application.DTO.Filters.Filter filters, GridFilter[] gridFilters, bool onlyEsg = false)
        {
            var query = QueryBuilder.ParseCustomFilter<GlobalPrimaryDeal>(gridFilters);

            query = query.WithTextFilters(nameof(GlobalPrimaryDeal.Isin), filters.Isins)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerName), filters.IssuerNames)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerSector), filters.IssuerSectors)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerCountry), filters.IssuerCountries)
             .WithTextFilters(nameof(GlobalPrimaryDeal.Currency), filters.Currency);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(GlobalPrimaryDeal.PricingDate), filters.TimePeriod);

            if (filters.Maturity != null)
                query = query.WithDateRangeFilters(nameof(GlobalPrimaryDeal.Maturity), filters.Maturity);

            if (onlyEsg)
            {
                query = query.WithBooleanFilters(nameof(GlobalPrimaryDeal.IsESG), true);
            }

            return query;
        }

        private static QueryContainer[] BuildReportQuery(DateTime startDate, DateTime endDate)
        {
            var query = QueryBuilder.DateRangeQueries(nameof(CartIndex.EventTime).ToLower(), startDate, endDate);

            return query.ToArray();
        }

        private static QueryContainer[] BuildEbookQuery(Application.DTO.Filters.Filter filters, GridFilter[] gridFilters,
            string[] salesUtCode)
        {
            var query = QueryBuilder.ParseCustomFilter<EbookOrderIndex>(gridFilters);

            query = query.WithTextFilters(nameof(EbookOrderIndex.Currency), filters.Currency)
                .WithTextFilters(nameof(EbookOrderIndex.Isin), filters.Isins)
                .WithTextFilters(nameof(EbookOrderIndex.IssuerSector), filters.IssuerSectors)
                .WithTextFilters(nameof(EbookOrderIndex.IssuerCountry), filters.IssuerCountries)
                .WithTextFilters(nameof(EbookOrderIndex.Issuer), filters.IssuerNames)
                .WithTextFilters(nameof(EbookOrderIndex.InvestorCountry), filters.InvestorCountries)
                .WithTextFilters(nameof(EbookOrderIndex.InvestorName), filters.InvestorNames)
                .WithTextFilters(nameof(EbookOrderIndex.SalesUtCode), salesUtCode);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(EbookOrderIndex.TradeDate), filters.TimePeriod);

            if (filters.Maturity != null)
                query = query.WithDateRangeFilters(nameof(EbookOrderIndex.MaturityDate), filters.TimePeriod);

            return query;
        }

        private static QueryContainer[] BuildEbookDealsQuery(Application.DTO.Filters.Filter filters, GridFilter[] gridFilters)
        {
            var query = QueryBuilder.ParseCustomFilter<EbookDealIndex>(gridFilters);

            query = query.WithTextFilters(nameof(EbookDealIndex.Currency), filters.Currency)
                .WithTextFilters(nameof(EbookDealIndex.Isin), filters.Isins)
                .WithTextFilters(nameof(EbookDealIndex.IssuerSector), filters.IssuerSectors)
                .WithTextFilters(nameof(EbookDealIndex.IssuerCountry), filters.IssuerCountries)
                .WithTextFilters(nameof(EbookDealIndex.Issuer), filters.IssuerNames);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(EbookDealIndex.TradeDate), filters.TimePeriod);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(EbookDealIndex.MaturityDate), filters.TimePeriod);

            return query;
        }

        private static QueryContainer[] BuildSecondaryFilterQuery(Application.DTO.Filters.Filter filters)
        {
            var query = Array.Empty<QueryContainer>();


            query = query.WithTextFilters(nameof(CartIndex.Isin), filters.Isins)
             .WithTextFilters(nameof(CartIndex.Verb), filters.Directions)
             .WithTextFilters(nameof(CartIndex.Currency), filters.Currency)
             .WithTextFilters(nameof(CartIndex.Issuer), filters.IssuerNames)
             .WithTextFilters(nameof(CartIndex.IssuerSector), filters.IssuerSectors)
             .WithTextFilters(nameof(CartIndex.IssuerCountry), filters.IssuerCountries)
             .WithTextFilters(nameof(CartIndex.InvestorName), filters.InvestorNames)
             .WithTextFilters(nameof(CartIndex.InvestorSector), filters.InvestorSectors)
             .WithTextFilters(nameof(CartIndex.InvestorCountry), filters.InvestorCountries)
             .WithTextFilters(nameof(CartIndex.AssetClassLvl1), filters.AssetClassLvl1)
             .WithTextFilters(nameof(CartIndex.AssetClassLvl2), filters.AssetClassLvl2)
             .WithTextFilters(nameof(CartIndex.AssetClassLvl3), filters.AssetClassLvl3)
             .WithTextFilters(nameof(CartIndex.AssetClassLvl4), filters.AssetClassLvl4)
             .WithTextFilters(nameof(CartIndex.AssetClassLvl5), filters.AssetClassLvl5);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(CartIndex.EventTime), filters.TimePeriod);

            if (filters.Maturity != null)
                query = query.WithDateRangeFilters(nameof(CartIndex.MaturityDate), filters.Maturity);

            return query;
        }

        private static QueryContainer[] BuildEbookDealsNestedQuery(Application.DTO.Filters.Filter filters, string[] salesUtCode, string nestedPath)
        {
            var query = Array.Empty<QueryContainer>();

            return query.WithTextFilters(nameof(EbookOrderInfo.InvestorName), filters.InvestorNames, nestedPath)
             .WithTextFilters(nameof(EbookOrderInfo.InvestorCountry), filters.InvestorCountries, nestedPath)
             .WithTextFilters(nameof(EbookOrderInfo.SalesUtCode), salesUtCode, nestedPath);
        }

        private static QueryContainer[] BuildNestedDealsSalesCodeQuery(string[] salesUtCode, string nestedPath = "")
            => QueryBuilder
                .MultiMatchPhraseQueries(nameof(EbookOrderInfo.SalesUtCode).ToLower(), salesUtCode, nestedPath)
                .ToArray();

        private static QueryContainer[] BuildQueryTopInvestor(Application.DTO.Filters.Filter filters, bool? isSeniorPreffered = null)
        {
            var query = Array.Empty<QueryContainer>();

            query = query.WithTextFilters(nameof(GlobalPrimaryDeal.Isin), filters.Isins)
             .WithTextFilters(nameof(GlobalPrimaryDeal.Currency), filters.Currency)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerName), filters.IssuerNames)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerSector), filters.IssuerSectors)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerCountry), filters.IssuerCountries);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(GlobalPrimaryDeal.PricingDate), filters.TimePeriod);

            if (filters.Maturity != null)
                query = query.WithDateRangeFilters(nameof(GlobalPrimaryDeal.Maturity), filters.Maturity);

            if (isSeniorPreffered.HasValue)
                query = query.WithBooleanFilters(nameof(GlobalPrimaryDeal.SeniorPreferred), isSeniorPreffered.Value);

            return query;

        }

        private static QueryContainer[] BuildSearchTopIssuesQuery(Application.DTO.Filters.Filter filters)
        {
            var query = Array.Empty<QueryContainer>();

            query = query.WithTextFilters(nameof(GlobalPrimaryDeal.Isin), filters.Isins)
             .WithTextFilters(nameof(GlobalPrimaryDeal.Currency), filters.Currency)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerName), filters.IssuerNames)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerSector), filters.IssuerSectors)
             .WithTextFilters(nameof(GlobalPrimaryDeal.IssuerCountry), filters.IssuerCountries);

            if (filters.TimePeriod != null)
                query = query.WithDateRangeFilters(nameof(GlobalPrimaryDeal.PricingDate), filters.TimePeriod);

            if (filters.Maturity != null)
                query = query.WithDateRangeFilters(nameof(GlobalPrimaryDeal.Maturity), filters.Maturity);

            return query;
        }
    }
}