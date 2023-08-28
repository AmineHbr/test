using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Vector.Application.DTO;
using Vector.Application.Queries.Ebook;
using Vector.Application.Queries.Primary;
using Vector.Application.Queries.Secondary;
using Vector.Common.Domain.Cart;
using Vector.Common.Domain.Ebook;
using Vector.Common.Domain.GlobalPrimaryMarket;

namespace Vector.Infrastructure.ElasticSearch.Services
{
    public interface IVectorElasticService
    {
        Task<PagedResult<CartIndex>> SearchSecondary(SearchCartQuery request, bool withOnlyExistingSales);

        Task<IEnumerable<DailyPotentiallyData>> GetReportData(DateTime startDate, DateTime endDate);

        Task<PagedResult<EbookOrderIndex>> SearchPrimary(SearchEbookQuery request);

        Task<PagedResult<EbookDealIndex>> SearchPrimaryDeals(SearchEbookDealsQuery request);

        Task<IEnumerable<string>> GetAllSecondaryPossibleValues(string propertyName, string query = null);

        Task<IEnumerable<string>> GetAllPrimaryPossibleValues(string propertyName, string query = null, bool isNested = false, string nestedPath = null);

        Task<IEnumerable<string>> GetAllEbookPossibleValues(string propertyName, string query = null);

        Task<IEnumerable<string>> AutoCompleteEbookIssuer(string query);

        Task<IEnumerable<LabelValue>> AutoCompleteCartDescription(string query);

        Task<IEnumerable<string>> AutoCompleteCartIssuer(string query);

        Task<IEnumerable<SecondaryFlowDataDto>> GetTopIssuers(GetSecondaryFlowDataQuery request);

        Task<IEnumerable<SummaryActivityDataDto>> GetSummaryActivity(GetSummaryActivityDataQuery request);

        Task<IEnumerable<CountryRepartitionDto>> GetCountryRepartition(string isin);

        Task<IEnumerable<TopInvestorDto>> GetTopInvestors(SearchTopInvestorQuery request);

        Task<PagedResult<GlobalPrimaryDeal>> GetIssues(SearchIssuesQuery query);

        Task<IEnumerable<TopIssuesDto>> SearchTopIssues(SearchTopIssuesRequestQuery request, string[] salesUtCodes);
        Task<BondDescriptionDto> SearchBondDescription(string isin);

        Task<LeagueTablesDto> GetLeagueTables(SearchLeagueTableQuery query);
    }
}