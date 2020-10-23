#************************************************************
# example trading strategy with d6tflow
# d6tflow is a framework for rapid prototyping and experiment tracking
# https://github.com/d6t/d6tflow
# see disclaimer at the end
#************************************************************

import d6tflow
import pandas as pd
import numpy as np
import pandas_datareader as pddr
import datetime

#************************************************************
# define workflow
#************************************************************

# get economic data
class GetDataEcon(d6tflow.tasks.TaskPqPandas):
    date_start = d6tflow.DateParameter() # define parameters
    date_end = d6tflow.DateParameter() # define parameters

    def run(self):
        df_gdp = pddr.DataReader('CPGDPAI', 'fred', self.date_start, self.date_end)
        self.save(df_gdp)

# generate l/s signals
@d6tflow.requires(GetDataEcon) # define dependency
class TradingSignals(d6tflow.tasks.TaskPqPandas):
    lookback_period = d6tflow.IntParameter()

    def run(self):
        df_gdp = self.inputLoad() # load input data

        # generate l/s trading signals
        df_signal = (df_gdp['CPGDPAI'].diff(self.lookback_period)>0)
        df_signal = df_signal.to_frame(name='position')
        df_signal['position'] = np.where(df_signal['position'],1,-1)

        self.save(df_signal)

# get stock prices
@d6tflow.requires(GetDataEcon)
class GetDataPx(d6tflow.tasks.TaskPqPandas):
    symbols = d6tflow.ListParameter()

    def run(self):
        df = pddr.DataReader(self.symbols, 'yahoo', self.date_start, self.date_end)
        df_rtn = df['Adj Close'].pct_change()
        self.save(df_rtn)

# run backtest
@d6tflow.requires(TradingSignals,GetDataPx)
class Backtest(d6tflow.tasks.TaskPqPandas):
    persist = ['portfolio','pnl']

    def run(self):
        df_signal = self.input()[0].load()
        df_rtn = self.input()[1].load()

        # combine signals and returns
        df_portfolio = pd.merge_asof(df_rtn, df_signal, left_index=True, right_index=True)

        # calc pnl
        df_pnl = df_portfolio[list(self.symbols)].multiply(df_portfolio['position'],axis=0)
        df_pnl = df_pnl.add_prefix('rtn_')

        self.save({'portfolio':df_portfolio,'pnl':df_pnl})

# for demo purposes only: reset everything at every run
import shutil
shutil.rmtree(d6tflow.settings.dirpath, ignore_errors=True)

#************************************************************
# define experiments
#************************************************************

strategy1 = dict(
    date_start=datetime.date(2018,1,1),
    date_end=datetime.date(2020,1,1),
    symbols = ['CAT','WMT'],
    lookback_period = 1
    )
strategy2 = strategy1.copy()
strategy2['symbols']=['MSFT','FB'] # run another universe
strategy3 = strategy1.copy()
strategy3['date_start']= datetime.date(2019,1,1) # run another time period

#************************************************************
# run backtests
#************************************************************

# run backtest including necessary dependencies
for istrat, strategy in enumerate([strategy1,strategy2,strategy3]):
    print(f'run strategy #{istrat+1}')
    print(d6tflow.preview(Backtest(**strategy)))  # show which tasks will be run
    d6tflow.run(Backtest(**strategy))
    df_pnl1 = Backtest(**strategy).output()['pnl'].load() # load task output
    print(f'pnl strategy #{istrat+1}:', df_pnl1.sum().sum().round(3))

def dev():
    TradingSignals(**strategy1).reset() # reset after making updates


#************************************************************
# backtest output
#************************************************************

'''
run strategy #1

 ===== Luigi Execution Preview ===== 


└─--[Backtest-{'date_start': '2018-01-01', 'date_end': '2020-01-01', 'lookback_period': '1', 'symbols': '["CAT", "WMT"]'} (PENDING)]
   |--[TradingSignals- (PENDING)]
   |  └─--[GetDataEcon- (PENDING)]
   └─--[GetDataPx- (PENDING)]
      └─--[GetDataEcon- (PENDING)]

 ===== Luigi Execution Preview ===== 

None

===== Luigi Execution Summary =====

Scheduled 4 tasks of which:
* 4 ran successfully:
    - 1 Backtest(date_start=2018-01-01, date_end=2020-01-01, lookback_period=1, symbols=["CAT", "WMT"])
    - 1 GetDataEcon(date_start=2018-01-01, date_end=2020-01-01)
    - 1 GetDataPx(date_start=2018-01-01, date_end=2020-01-01, symbols=["CAT", "WMT"])
    - 1 TradingSignals(date_start=2018-01-01, date_end=2020-01-01, lookback_period=1)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

pnl strategy #1: -0.029
run strategy #2

 ===== Luigi Execution Preview ===== 


└─--[Backtest-{'date_start': '2018-01-01', 'date_end': '2020-01-01', 'lookback_period': '1', 'symbols': '["MSFT", "FB"]'} (PENDING)]
   |--[TradingSignals- (COMPLETE)]
   |  └─--[GetDataEcon- (COMPLETE)]
   └─--[GetDataPx- (PENDING)]
      └─--[GetDataEcon- (COMPLETE)]

 ===== Luigi Execution Preview ===== 

None

===== Luigi Execution Summary =====

Scheduled 4 tasks of which:
* 2 complete ones were encountered:
    - 1 GetDataEcon(date_start=2018-01-01, date_end=2020-01-01)
    - 1 TradingSignals(date_start=2018-01-01, date_end=2020-01-01, lookback_period=1)
* 2 ran successfully:
    - 1 Backtest(date_start=2018-01-01, date_end=2020-01-01, lookback_period=1, symbols=["MSFT", "FB"])
    - 1 GetDataPx(date_start=2018-01-01, date_end=2020-01-01, symbols=["MSFT", "FB"])

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

pnl strategy #2: -0.16
run strategy #3

 ===== Luigi Execution Preview ===== 


└─--[Backtest-{'date_start': '2019-01-01', 'date_end': '2020-01-01', 'lookback_period': '1', 'symbols': '["CAT", "WMT"]'} (PENDING)]
   |--[TradingSignals- (PENDING)]
   |  └─--[GetDataEcon- (PENDING)]
   └─--[GetDataPx- (PENDING)]
      └─--[GetDataEcon- (PENDING)]

 ===== Luigi Execution Preview ===== 

None

===== Luigi Execution Summary =====

Scheduled 4 tasks of which:
* 4 ran successfully:
    - 1 Backtest(date_start=2019-01-01, date_end=2020-01-01, lookback_period=1, symbols=["CAT", "WMT"])
    - 1 GetDataEcon(date_start=2019-01-01, date_end=2020-01-01)
    - 1 GetDataPx(date_start=2019-01-01, date_end=2020-01-01, symbols=["CAT", "WMT"])
    - 1 TradingSignals(date_start=2019-01-01, date_end=2020-01-01, lookback_period=1)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

pnl strategy #3: -0.449

'''
#************************************************************
# Disclaimer
# These materials, and any other information or data conveyed in connection with these materials, is intended for informational purposes only. Under no circumstances are these materials, or any information or data conveyed in connection with such report, to be considered an offer or solicitation of an offer to buy or sell any securities of any company. Nor may these materials, or any information or data conveyed in connection with such report, be relied on in any manner as legal, tax or investment advice. The information and data is not intended to be used as the primary basis of investment decisions and nothing contained herein or conveyed in connection therewith is, or is intended to be, predictive of the movement of the market prices of the securities of the applicable company or companies. The facts and opinions presented are those of the author only and not official opinions of any financial instituion.
#************************************************************

# interactive notebook 
# https://mybinder.org/v2/gh/d6tdev/d6tflow-binder-interactive/master?filepath=example-trading.ipynb


