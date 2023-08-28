using Nest;
using System;
using System.Linq;
using Vector.Application.DTO.Searches;
using Vector.Common.Domain.Ebook;

namespace Vector.Infrastructure.ElasticSearch
{
    public static class SortBuilder
    {
        public static SortDescriptor<T> BuildSortDescriptor<T>(GridSort[] gridSorting,
            SortDescriptor<T> q) where T : class
        {
            var sortDescriptor = q;

            var sortFields = gridSorting.ToList();
            foreach (var item in sortFields)
            {
                var prefix = string.Empty;
                var propertyName = item.ColId.ToLower();
                var property = Array.Find((typeof(T)).GetProperties(), p => p.Name.ToLower() == propertyName);


                if (property != null)
                {

                    if (property.Name == nameof(EbookOrderIndex.InitialOrders))
                    {
                        sortDescriptor = sortDescriptor.Field(fd =>fd.Field(nameof(EbookOrderIndex.SumInitialOrders).ToLower()).Order(item.Sort == "asc" ? SortOrder.Ascending : SortOrder.Descending));
                    }
                   else
                   {
                      sortDescriptor = sortDescriptor.Field(fd =>
                          fd.Field(property.PropertyType == typeof(string) ? $"{propertyName}.keyword" : $"{propertyName}")
                             .Order(item.Sort == "asc" ? SortOrder.Ascending : SortOrder.Descending));
                   }    

                 }
            }

            return sortDescriptor;
        }
    }
}