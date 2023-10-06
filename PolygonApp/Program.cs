using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
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

            Action<BaseData> callback = (dataPoint) =>
            {
                if (dataPoint != null)
                {
                    Log.Trace($"\n\n{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}\n");
                }
            };

            foreach (var config in configs)
            {
                ProcessFeed(polygon.Subscribe(config, (sender, args) =>
                {
                    var dataPoint = ((NewDataAvailableEventArgs)args).DataPoint;
                    Log.Trace($"{dataPoint}. Time span: {dataPoint.Time} - {dataPoint.EndTime}");
                }), callback);
            }

            Console.ReadKey();

            polygon.Dispose();

            Log.Trace($"End Time Latencies: {string.Join(", ", polygon.Latencies)}");
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

        private static Task ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null)
        {
            return Task.Run(() =>
            {
                try
                {
                    while (enumerator.MoveNext())
                    {
                        var tick = enumerator.Current;
                        callback?.Invoke(tick);
                    }
                }
                catch (Exception err)
                {
                    Log.Error(err.ToString());
                }

                Log.Trace("Exiting process feed thread");
            });
        }
    }
}
