using System;
using RestSharp;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    public class PolygonAdditionalTests
    {
        private PolygonRestApiClient _restApiClient = new PolygonRestApiClient(Config.Get("polygon-api-key"));

        [TestCase(5)]
        public void GetOptionChainRequestTimeout(int amountRequest)
        {
            var referenceDate = new DateTime(2024, 08, 16);
            var underlying = Symbol.Create("AAPL", SecurityType.Index, Market.USA);

            var request = new RestRequest("/v3/reference/options/contracts", Method.GET);
            request.AddQueryParameter("underlying_ticker", underlying.Value);
            request.AddQueryParameter("as_of", referenceDate.ToStringInvariant("yyyy-MM-dd"));
            request.AddQueryParameter("limit", "1000");

            do
            {
                foreach (var contracts in _restApiClient.DownloadAndParseData<OptionChainResponse>(request))
                {
                    Assert.IsTrue(contracts.Results.Any());
                } 
            } while (--amountRequest > 0);
        }
    }
}
