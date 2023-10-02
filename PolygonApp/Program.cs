using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace QuantConnect.Polygon.App
{
    public class Program
    {
        static void Main(string[] args)
        {
            var polygon = new PolygonDataQueueHandler();
            var unsubscribed = false;

            var configs = new[]
            {
                GetSubscriptionDataConfig<TradeBar>(Symbol.CreateOption(Symbol.Create("SPY", SecurityType.Equity, Market.USA), Market.USA,
                    OptionStyle.American, OptionRight.Call, 150m, new DateTime(2023, 10, 20)), Resolution.Minute),
                GetSubscriptionDataConfig<TradeBar>(Symbol.CreateOption(Symbol.Create("AAPL", SecurityType.Equity, Market.USA), Market.USA,
                    OptionStyle.American, OptionRight.Call, 55m, new DateTime(2023, 10, 20)), Resolution.Minute)
            };

            Action<BaseData> callback = (dataPoint) =>
            {
                //if (dataPoint == null)
                //    return;

                //Console.WriteLine($"{dataPoint} Time span: {dataPoint.Time} - {dataPoint.EndTime}");

                //if (unsubscribed && dataPoint.Symbol.Value == "AAPL")
                //{
                //    throw new Exception("Should not receive data for unsubscribed symbol");
                //}
            };

            foreach (var config in configs)
            {
                ProcessFeed(polygon.Subscribe(config, (sender, args) => { }), callback);
            }

            Thread.Sleep(3 * 60 * 1000);

            //polygon.Unsubscribe(configs.First(config => config.Symbol.Underlying.Value == "AAPL"));

            //Console.WriteLine("Unsubscribing");

            //Thread.Sleep(2000);
            // some messages could be inflight, but after a pause all MBLY messages must have beed consumed by ProcessFeed
            unsubscribed = true;

            //Thread.Sleep(3 * 60 * 1000);
            polygon.Dispose();

            //Console.WriteLine($"Start Time Latencies: {string.Join(", ", polygon.StartTimeLatencies)}");
            //Console.WriteLine($"End Time Latencies: {string.Join(", ", polygon.EndTimeLatencies)}");
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

        private static void ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null)
        {
            return;
            Task.Run(() =>
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
                    Log.Error(err.Message);
                }
            });
        }
    }
}
