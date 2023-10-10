using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System;
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;

namespace QuantConnect.Polygon.App
{
    public class Program
    {
        static void Main(string[] args)
        {
            var polygon = new PolygonDataQueueHandler();

            var configs = new[]
            {
                GetSubscriptionDataConfig<TradeBar>(Symbol.CreateOption(Symbol.Create("SPY", SecurityType.Equity, Market.USA), Market.USA,
                    OptionStyle.American, OptionRight.Call, 150m, new DateTime(2023, 10, 20)), Resolution.Minute),
                GetSubscriptionDataConfig<TradeBar>(Symbol.CreateOption(Symbol.Create("AAPL", SecurityType.Equity, Market.USA), Market.USA,
                    OptionStyle.American, OptionRight.Call, 55m, new DateTime(2023, 10, 20)), Resolution.Minute)
            };

            foreach (var config in configs)
            {
                polygon.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
                });
            }

            Console.ReadKey();

            polygon.Dispose();
        }

        private static SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
        {
            return new SubscriptionDataConfig(
                typeof(T),
                symbol,
                resolution,
                TimeZones.Utc,
                TimeZones.Utc,
                true,
                true,
                false);
        }
    }
}
