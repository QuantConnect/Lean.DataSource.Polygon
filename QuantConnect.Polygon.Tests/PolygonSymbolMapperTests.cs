/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Logging;
using QuantConnect.Polygon;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    public class PolygonSymbolMapperTests
    {
        private static TestCaseData[] PolygonToLeanOptionSymbolConversionTestCases => new TestCaseData[]
        {
            new TestCaseData("O:SPY160219C00192000", Symbols.SPY_C_192_Feb19_2016),
            new TestCaseData("O:SPY160219P00192000", Symbols.SPY_P_192_Feb19_2016),
            new TestCaseData("O:AAPL231020C00080550",
                Symbol.CreateOption(Symbols.AAPL, Market.USA, OptionStyle.American, OptionRight.Call, 80.55m, new DateTime(2023, 10, 20))),
            new TestCaseData("O:AAPL231020P00080550",
                Symbol.CreateOption(Symbols.AAPL, Market.USA, OptionStyle.American, OptionRight.Put, 80.55m, new DateTime(2023, 10, 20)))
        };

        [TestCaseSource(nameof(PolygonToLeanOptionSymbolConversionTestCases))]
        public void ConvertsPolygonOptionSymbolToLeanSymbol(string polygonSymbol, Symbol leanSymbol)
        {
            var mapper = new PolygonSymbolMapper();
            var convertedSymbol = mapper.GetLeanOptionSymbol(polygonSymbol);
            Assert.That(convertedSymbol, Is.EqualTo(leanSymbol));
        }
    }
}