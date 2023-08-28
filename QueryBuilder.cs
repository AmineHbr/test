using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Vector.Application.DTO;
using Vector.Application.DTO.Filters;
using Vector.Application.DTO.Searches;
using Vector.Common.Domain.Cart;
using Vector.Common.Domain.Ebook;
using Vector.Infrastructure.Helpers;
using ValueType = Vector.Application.DTO.Searches.ValueType;

namespace Vector.Infrastructure.ElasticSearch
{
    public static class QueryBuilder
    {
        public static QueryContainer[] MultiMatchQueries(string field, string[] param, string pathNested = "")
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            foreach (var item in param)
            {
                var splittedString = Regex.Replace(item.Replace("-", "").Replace(",", "").Replace("(", "").Replace(")", "").Replace(".", ""), @"\s+", " ").Split(' ');

                List<QueryContainer> subQueryContainerList = new List<QueryContainer>();
                foreach (var term in splittedString)
                {
                    subQueryContainerList.Add(new MatchQuery() { Field = pathNested.Length == 0 ? field : $"{pathNested}.{field}", Query = term });
                }

                BoolQuery subQuery = new BoolQuery()
                {
                    Must = subQueryContainerList
                };

                queryContainerList.Add(subQuery);
            }

            BoolQuery query = new BoolQuery()
            {
                Should = queryContainerList
            };

            return new QueryContainer[] { new QueryContainer(query) };
        }

        public static QueryContainer[] WithTextFilters(this QueryContainer[] query, string field, string[] param, string pathNested = "")
        {
            return query.Concat(MultiMatchPhraseQueries(field, param, pathNested)).ToArray();
        }

        public static QueryContainer[] WithDateRangeFilters(this QueryContainer[] query, string field, DateRangeFilter filter)
        {
            return query.Concat(DateRangeQueries(field, filter)).ToArray();
        }

        public static QueryContainer[] WithBooleanFilters(this QueryContainer[] query, string field, bool value)
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            QueryContainer dateRangeQuery = new TermQuery() { Field = field.ToLower(), Value = value };
            queryContainerList.Add(dateRangeQuery);

            return query.Concat(queryContainerList).ToArray();
        }

        public static QueryContainer[] MultiMatchPhraseQueries(string field, string[] param, string pathNested = "")
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            foreach (var item in param)
            {
                List<QueryContainer> subQueryContainerList = new List<QueryContainer>
                {
                    new MatchPhraseQuery() { Field = pathNested.Length == 0 ? field.ToLower() : $"{pathNested}.{field.ToLower()}", Query = item }
                };

                BoolQuery subQuery = new BoolQuery()
                {
                    Must = subQueryContainerList
                };

                queryContainerList.Add(subQuery);
            }

            BoolQuery query = new BoolQuery()
            {
                Should = queryContainerList
            };

            return new QueryContainer[] { new QueryContainer(query) };
        }

        public static QueryContainer[] MultiTermQueries(string field, string[] param)
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            foreach (var item in param)
            {
                queryContainerList.Add(new TermQuery() { Field = field, Value = item });
            }

            BoolQuery query = new BoolQuery()
            {
                Should = queryContainerList
            };

            return new QueryContainer[] { new QueryContainer(query) };
        }

        public static QueryContainer[] MultiWildcardQueries(string field, string[] param)
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            foreach (var item in param)
            {
                var splittedString = item.Split(' ');
                List<QueryContainer> subQueryContainerList = new List<QueryContainer>();
                foreach (var term in splittedString)
                {
                    subQueryContainerList.Add(new WildcardQuery() { Field = $"{field}.keyword", Value = $"*{EscapeWildCardQuery(term)}*", CaseInsensitive = true, });
                }

                BoolQuery subQuery = new BoolQuery()
                {
                    Must = subQueryContainerList
                };

                queryContainerList.Add(subQuery);
            }

            BoolQuery query = new BoolQuery()
            {
                Should = queryContainerList
            };

            return new[] { new QueryContainer(query) };
        }

        private static string EscapeWildCardQuery(string query)
        {
            string[] specialCharacters = { ".", "+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "~", "*", "?", ":", "\"", "\\" };

            foreach (var specialChar in specialCharacters)
            {
                query = query.Replace(specialChar, $"\\{specialChar}");
            }

            return query;
        }

        public static QueryContainer[] DateRangeQueries(string field, DateRangeFilter dateRangeFilter)
        {
            return DateRangeQueries(field, dateRangeFilter.Value.StartDate, dateRangeFilter.Value.EndDate);
        }

        public static QueryContainer[] DateRangeQueries(string field, DateTime? gt, DateTime? lt)
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            QueryContainer dateRangeQuery = new DateRangeQuery() { Field = field.ToLower(), GreaterThanOrEqualTo = gt, LessThanOrEqualTo = lt };
            queryContainerList.Add(dateRangeQuery);

            return queryContainerList.ToArray();
        }

        public static QueryContainer GetFilterQuery<T>(ValueType type, string field, FilterDto filter)
        {
            if (type == ValueType.DateTime)
            {
                DateTime? gt = filter.ValueStart == null ? null : DateTime.Parse(filter.ValueStart);
                DateTime? lt = filter.ValueEnd == null ? null : DateTime.Parse(filter.ValueEnd);
                return DateQueries(field, filter.FilterType, gt, lt);
            }
            if (type == ValueType.Number)
            {
                double? gt = double.Parse(filter.ValueStart);
                double? lt = filter.ValueEnd == null ? null : double.Parse(filter.ValueEnd);
                // Transform value for Nominal and Allocation on EbookIndex
                if (typeof(T) == typeof(EbookOrderIndex) && (field == nameof(EbookOrderIndex.TradeAmount).ToLower()))
                {
                    gt /= 1000000;
                    lt /= 1000000;
                }
                return NumericQueries(field, filter.FilterType, gt, lt);
            }
            if (type == ValueType.String)
            {
                return TextQueries(field, filter.FilterType, filter.ValueStart);
            }
            return null;
        }

        public static QueryContainer DateQueries(string field, FilterType type, DateTime? valueStart, DateTime? valueEnd)
        {
            if (type == FilterType.Equals)
            {
                return new DateRangeQuery() { Field = field, GreaterThanOrEqualTo = valueStart.Value, LessThan = valueStart.Value.AddDays(1) };
            }
            if (type == FilterType.GreaterThan)
            {
                return new DateRangeQuery() { Field = field, GreaterThan = valueStart };
            }
            if (type == FilterType.LessThan)
            {
                return new DateRangeQuery() { Field = field, LessThan = valueStart };
            }
            if (type == FilterType.NotEquals)
            {
                return !new DateRangeQuery() { Field = field, GreaterThanOrEqualTo = valueStart.Value, LessThan = valueStart.Value.AddDays(1) };
            }
            if (type == FilterType.InRange)
            {
                return new DateRangeQuery() { Field = field, GreaterThanOrEqualTo = valueStart, LessThanOrEqualTo = valueEnd };
            }
            if (type == FilterType.Blank)
            {
                return !new ExistsQuery() { Field = field };
            }
            if (type == FilterType.NotBlank)
            {
                return new ExistsQuery() { Field = field };
            }
            return null;
        }

        public static QueryContainer NumericQueries(string field, FilterType type, double? valueStart, double? valueEnd)
        {
            if (type == FilterType.Equals)
            {
                return new TermQuery() { Field = field, Value = valueStart };
            }
            if (type == FilterType.GreaterThan)
            {
                return new NumericRangeQuery() { Field = field, GreaterThan = valueStart };
            }
            if (type == FilterType.LessThan)
            {
                return new NumericRangeQuery() { Field = field, LessThan = valueStart };
            }
            if (type == FilterType.NotEquals)
            {
                return !new TermQuery() { Field = field, Value = valueStart };
            }
            if (type == FilterType.GreaterThanOrEqual)
            {
                return new NumericRangeQuery() { Field = field, GreaterThanOrEqualTo = valueStart };
            }
            if (type == FilterType.LessThanOrEqual)
            {
                return new NumericRangeQuery() { Field = field, LessThanOrEqualTo = valueStart };
            }
            if (type == FilterType.InRange)
            {
                return new NumericRangeQuery() { Field = field, GreaterThanOrEqualTo = valueStart, LessThanOrEqualTo = valueEnd };
            }
            return null;
        }

        public static QueryContainer TextQueries(string field, FilterType type, string param, string valueEnd = null)
        {
            if (type == FilterType.Contains)
            {
                if (!param.Contains(' '))
                {
                    return new QueryStringQuery() { Fields = field, Query = $"*{param}*" };
                }
                var queries = MultiWildcardQueries(field, new string[] { param });

                return queries.FirstOrDefault();
            }
            if (type == FilterType.NotContains)
            {
                return !new QueryStringQuery() { Fields = field, Query = $"*{param}*" };
            }
            if (type == FilterType.Equals)
            {
                if (!param.Contains(' '))
                {
                    return new MatchQuery() { Field = field, Query = param };
                }
                var queries = MultiMatchPhraseQueries(field, new string[] { param });
                if (queries.Length > 0)
                {
                    return queries[0];
                }
                return null;
            }
            if (type == FilterType.NotEquals)
            {
                return !new MatchQuery() { Field = field, Query = param };
            }
            if (type == FilterType.StartsWith)
            {
                return new QueryStringQuery() { Fields = field, Query = $"{param}*" };
            }
            if (type == FilterType.EndsWith)
            {
                return new QueryStringQuery() { Fields = field, Query = $"*{param}" };
            }
            if (type == FilterType.GreaterThan)
            {
                return new TermRangeQuery() { Field = field, GreaterThan = param };
            }
            if (type == FilterType.InRange)
            {
                return new TermRangeQuery() { Field = field, GreaterThan = param, LessThan = valueEnd };
            }
            if (type == FilterType.Blank)
            {
                return !new ExistsQuery() { Field = field };
            }
            if (type == FilterType.NotBlank)
            {
                return new ExistsQuery() { Field = field };
            }
            return null;
        }

        public static QueryContainer[] ExistingFieldQuery(string field, string pathNested = "")
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            ExistsQuery eQuery = new ExistsQuery() { Field = pathNested.Length == 0 ? field : $"{pathNested}.{field}" };
            queryContainerList.Add(eQuery);

            return queryContainerList.ToArray();
        }
        public static QueryContainer[] ParseCustomFilter<T>(GridFilter[] gridFilters, string pathNested = "")
        {
            List<QueryContainer> queryContainerList = new List<QueryContainer>();

            foreach (GridFilter dto in gridFilters)
            {
                var fieldName = pathNested.Length == 0 ? dto.Field.ToLower() : $"{pathNested}.{dto.Field.ToLower()}";

                if (typeof(T) == typeof(EbookOrderIndex) && fieldName == nameof(EbookOrderIndex.InitialOrders).ToLower())
                {
                    fieldName = nameof(EbookOrderIndex.SumInitialOrders).ToLower();
                }
                // Secondary: tradedate (date in string format) -> eventtime (true date fromat)
                if (typeof(T) == typeof(CartIndex) && fieldName == nameof(CartIndex.TradeDate).ToLower())
                {
                    fieldName = nameof(CartIndex.EventTime).ToLower();
                }

                if (dto.Operator == FilterOperator.None && dto.ValueType != ValueType.Set)
                {
                    foreach (FilterDto filter in dto.Filters)
                    {
                        queryContainerList.Add(QueryBuilder.GetFilterQuery<T>(dto.ValueType, fieldName, filter));
                    }
                }
                else if (dto.Operator == FilterOperator.None && dto.ValueType == ValueType.Set)
                {
                    List<QueryContainer> shouldQueryContainerList = new List<QueryContainer>();

                    foreach (FilterDto filter in dto.Filters)
                    {
                        if (fieldName == nameof(CartIndex.MarketGtw).ToLower())
                        {
                            var listMarketGtw = MarketGtwMatcher.GetMarketGtwFromSys(filter.ValueStart);
                            listMarketGtw.Select(v => new FilterDto()
                            {
                                FilterType = filter.FilterType,
                                ValueStart = v,
                                ValueEnd = null
                            }).ToList().ForEach(f =>
                                shouldQueryContainerList.Add(
                                    QueryBuilder.GetFilterQuery<T>(ValueType.String, fieldName, f)));
                        }
                        else if (fieldName == nameof(GlobalViewIsinDetailDto.Origin).ToLower())
                        {
                            if (typeof(T) == typeof(CartIndex) && filter.ValueStart != "PRI")
                            {
                                shouldQueryContainerList.Add(
                                    QueryBuilder.GetFilterQuery<T>(ValueType.String, "verb", filter));
                            }
                        }
                        else
                        {
                            shouldQueryContainerList.Add(
                                QueryBuilder.GetFilterQuery<T>(ValueType.String, fieldName, filter));
                        }
                    }

                    BoolQuery query = new BoolQuery()
                    {
                        Should = shouldQueryContainerList,
                    };
                    queryContainerList.Add(query);
                }
                else if (dto.Operator == FilterOperator.And && dto.Filters.Length == 2)
                {
                    queryContainerList.Add(
                        QueryBuilder.GetFilterQuery<T>(dto.ValueType, fieldName, dto.Filters[0]) &&
                        QueryBuilder.GetFilterQuery<T>(dto.ValueType, fieldName, dto.Filters[1])
                    );
                }
                else if (dto.Operator == FilterOperator.Or && dto.Filters.Length == 2)
                {
                    queryContainerList.Add(
                        QueryBuilder.GetFilterQuery<T>(dto.ValueType, fieldName, dto.Filters[0]) ||
                        QueryBuilder.GetFilterQuery<T>(dto.ValueType, fieldName, dto.Filters[1])
                    );
                }
            }

            return queryContainerList.ToArray();
        }
    }
}
