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
using QuantConnect.Polygon;
using System;

namespace QuantConnect.Tests.Polygon
{
    [TestFixture]
    public class PolygonSymbolMapperTests
    {
        private static TestCaseData[] PolygonToLeanSymbolConversionTestCases => new TestCaseData[]
        {
            // Equity
            new TestCaseData("SPY", Symbols.SPY),
            new TestCaseData("AAPL", Symbols.AAPL),
            new TestCaseData("IBM", Symbols.IBM),

            // Options
            new TestCaseData("O:SPY160219C00192000", Symbols.SPY_C_192_Feb19_2016),
            new TestCaseData("O:SPY160219P00192000", Symbols.SPY_P_192_Feb19_2016),
            new TestCaseData("O:AAPL231020C00080550",
                Symbol.CreateOption(Symbols.AAPL, Market.USA, OptionStyle.American, OptionRight.Call, 80.55m, new DateTime(2023, 10, 20))),
            new TestCaseData("O:AAPL231020P00080550",
                Symbol.CreateOption(Symbols.AAPL, Market.USA, OptionStyle.American, OptionRight.Put, 80.55m, new DateTime(2023, 10, 20)))
        };

        [TestCaseSource(nameof(PolygonToLeanSymbolConversionTestCases))]
        public void ConvertsPolygonSymbolToLeanSymbol(string polygonSymbol, Symbol leanSymbol)
        {
            var mapper = new PolygonSymbolMapper();
            var convertedSymbol = mapper.GetLeanSymbol(polygonSymbol);
            Assert.That(convertedSymbol, Is.EqualTo(leanSymbol));
        }

        [TestCase("O:SPY240104C00467000", SecurityType.Option, ExpectedResult = SecurityType.Equity)]
        [TestCase("O:SPX240104C04685000", SecurityType.IndexOption, ExpectedResult = SecurityType.Index)]
        [TestCase("O:SPXW240104C04685000", SecurityType.IndexOption, ExpectedResult = SecurityType.Index)]
        public SecurityType ConvertsOptionSymbolWithCorrectUnderlyingSecurityType(string ticker, SecurityType optionType)
        {
            var mapper = new PolygonSymbolMapper();
            var symbol = mapper.GetLeanSymbol(ticker, optionType, Market.USA, new DateTime(2024, 01, 04), 400m, OptionRight.Call);

            return symbol.Underlying.SecurityType;
        }

        [Test]
        public void ConvertsWeeklyIndexOptionSymbolWithParameters()
        {
            var mapper = new PolygonSymbolMapper();
            var expiry = new DateTime(2024, 01, 04);
            var strike = 4685m;
            // The underlying symbol should be specified for weekly index options,
            // otherwise the underlying will be set to SPXW/VIXW/... when it should actually be SPX/VIX/...
            var symbol = mapper.GetLeanSymbol("O:SPXW240104C04685000", SecurityType.IndexOption, Market.USA, OptionStyle.American, expiry, strike,
                OptionRight.Call, underlying: Symbols.SPX);

            Assert.That(symbol.Underlying, Is.EqualTo(Symbols.SPX));
            Assert.That(symbol, Is.EqualTo(Symbol.CreateOption(Symbols.SPX, "SPXW", Market.USA, OptionStyle.American, OptionRight.Call, strike, expiry)));
            Assert.That(symbol.ID.Date, Is.EqualTo(expiry));
            Assert.That(symbol.ID.OptionRight, Is.EqualTo(OptionRight.Call));
            Assert.That(symbol.ID.OptionStyle, Is.EqualTo(OptionStyle.American));
            Assert.That(symbol.ID.StrikePrice, Is.EqualTo(strike));
        }

        [Test]
        public void ConvertsWeeklyIndexOptionSymbolWithoutParameters()
        {
            var mapper = new PolygonSymbolMapper();
            var expiry = new DateTime(2024, 01, 04);
            var strike = 4685m;
            // The underlying symbol should be specified for weekly index options,
            // otherwise the underlying will be set to SPXW/VIXW/... when it should actually be SPX/VIX/...
            var symbol = mapper.GetLeanSymbol("O:SPXW240104C04685000");

            Assert.That(symbol.Underlying, Is.EqualTo(Symbols.SPX));
            Assert.That(symbol, Is.EqualTo(Symbol.CreateOption(Symbols.SPX, "SPXW", Market.USA, OptionStyle.American, OptionRight.Call, strike, expiry)));
            Assert.That(symbol.ID.Date, Is.EqualTo(expiry));
            Assert.That(symbol.ID.OptionRight, Is.EqualTo(OptionRight.Call));
            Assert.That(symbol.ID.OptionStyle, Is.EqualTo(OptionStyle.American));
            Assert.That(symbol.ID.StrikePrice, Is.EqualTo(strike));
        }

        [Test]
        public void ConvertsLeanIndexSymbolToPolygon()
        {
            var mapper = new PolygonSymbolMapper();
            var symbol = Symbol.Create("SPX", SecurityType.Index, Market.USA);
            var polygonSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.That(polygonSymbol, Is.EqualTo("I:SPX"));
        }

        [Test]
        public void ConvertsPolygonIndexSymbolToLean()
        {
            var mapper = new PolygonSymbolMapper();
            var symbol = mapper.GetLeanSymbol("I:SPX");
            Assert.That(symbol, Is.EqualTo(Symbol.Create("SPX", SecurityType.Index, Market.USA)));
        }

        [Test]
        public void ConvertsLeanEquitySymbolToPolygon()
        {
            var mapper = new PolygonSymbolMapper();
            var symbol = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);
            var polygonSymbol = mapper.GetBrokerageSymbol(symbol);
            Assert.That(polygonSymbol, Is.EqualTo("AAPL"));
        }

        [Test]
        public void ConvertsPolygonEquitySymbolToLean()
        {
            var mapper = new PolygonSymbolMapper();
            var symbol = mapper.GetLeanSymbol("AAPL");
            Assert.That(symbol, Is.EqualTo(Symbol.Create("AAPL", SecurityType.Equity, Market.USA)));
        }
    }
}