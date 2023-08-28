namespace Vector.Infrastructure.ElasticSearch.Services
{
    public class DailyPotentiallyData
    {
        public string AssetClassLvl3 { get; set; }
        public string AssetClassLvl4 { get; set; }
        public string Currency { get; set; }
        public double PotVol => BidPotVol + OfferPotVol;
        public double TradVol => BidTradVol + OfferTradVol;
        public double Hit => PotVol != 0 ? TradVol / PotVol : 0;

        public double BidPotVol { get; set; }

        public double BidTradVol { get; set; }

        public double BidHit => BidPotVol != 0 ? BidTradVol / BidPotVol : 0;

        public double OfferPotVol { get; set; }

        public double OfferTradVol { get; set; }

        public double OfferHit => OfferPotVol != 0 ? OfferTradVol / OfferPotVol : 0;

        public double AvgNbTraders { get; set; }
    }
}